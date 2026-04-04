using System.IO.MemoryMappedFiles;

using MimironSQL.Formats;

namespace MimironSQL.Formats.Wdc5.Index;

internal sealed class Db2IndexReader : IDisposable
{
    private readonly MemoryMappedFile _mmf;
    private readonly MemoryMappedViewAccessor _accessor;
    private readonly byte[] _pageBuf;
    private bool _disposed;

    private readonly long _rootPageOffset;
    private readonly int _treeHeight;
    private readonly byte _valueByteWidth;

    internal int RecordCount { get; }
    internal int TreeHeight => _treeHeight;
    internal byte ValueByteWidth => _valueByteWidth;

    public Db2IndexReader(string filePath)
    {
        _mmf = MemoryMappedFile.CreateFromFile(filePath, FileMode.Open, mapName: null, 0, MemoryMappedFileAccess.Read);
        _accessor = _mmf.CreateViewAccessor(0, 0, MemoryMappedFileAccess.Read);
        _pageBuf = new byte[Db2IndexFileFormat.PageSize];

        ReadPage(0);

        var magic = Db2IndexFileFormat.ReadUInt32(_pageBuf, Db2IndexFileFormat.HdrMagicOffset);
        if (magic != Db2IndexFileFormat.Magic)
        {
            throw new InvalidDataException($"Invalid .db2idx magic: 0x{magic:X8}. Expected 0x{Db2IndexFileFormat.Magic:X8}.");
        }

        var version = Db2IndexFileFormat.ReadInt32(_pageBuf, Db2IndexFileFormat.HdrVersionOffset);
        if (version != Db2IndexFileFormat.FormatVersion)
        {
            throw new InvalidDataException($"Unsupported .db2idx format version {version}. Expected {Db2IndexFileFormat.FormatVersion}.");
        }

        _rootPageOffset = Db2IndexFileFormat.ReadInt64(_pageBuf, Db2IndexFileFormat.HdrRootPageOffsetOffset);
        _treeHeight = Db2IndexFileFormat.ReadInt32(_pageBuf, Db2IndexFileFormat.HdrTreeHeightOffset);
        _valueByteWidth = _pageBuf[Db2IndexFileFormat.HdrValueByteWidthOffset];
        RecordCount = Db2IndexFileFormat.ReadInt32(_pageBuf, Db2IndexFileFormat.HdrRecordCountOffset);
    }

    public List<RowHandle> FindEquals(ulong encodedTarget)
    {
        var results = new List<RowHandle>();

        if (_treeHeight == 0 || _rootPageOffset == Db2IndexFileFormat.EmptyRootOffset)
        {
            return results;
        }

        var pageOffset = NavigateToLeaf(encodedTarget);
        if (pageOffset < 0)
        {
            return results;
        }

        while (true)
        {
            ReadPage(pageOffset);

            if (_pageBuf[Db2IndexFileFormat.LeafTypeOffset] != Db2IndexFileFormat.PageTypeLeaf)
            {
                break;
            }

            var entryCount = Db2IndexFileFormat.ReadUInt16(_pageBuf, Db2IndexFileFormat.LeafEntryCountOffset);
            var nextPageOffset = Db2IndexFileFormat.ReadInt64(_pageBuf, Db2IndexFileFormat.LeafNextPageOffset);
            var entrySize = Db2IndexFileFormat.GetEntrySize(_valueByteWidth);
            var pos = Db2IndexFileFormat.LeafEntriesOffset;

            for (var i = 0; i < entryCount; i++)
            {
                var key = Db2IndexFileFormat.ReadKey(_pageBuf, pos, _valueByteWidth);
                if (key == encodedTarget)
                {
                    var sectionIndex = Db2IndexFileFormat.ReadUInt16(_pageBuf, pos + _valueByteWidth);
                    var rowIndex = Db2IndexFileFormat.ReadInt32(_pageBuf, pos + _valueByteWidth + 2);
                    results.Add(new RowHandle(sectionIndex, rowIndex, rowId: 0));
                }
                else if (key > encodedTarget)
                {
                    return results;
                }

                pos += entrySize;
            }

            if (nextPageOffset == 0)
            {
                break;
            }

            pageOffset = nextPageOffset;
        }

        return results;
    }

    public List<RowHandle> FindRange(ulong loEncoded, ulong hiEncoded)
    {
        var results = new List<RowHandle>();

        if (_treeHeight == 0 || _rootPageOffset == Db2IndexFileFormat.EmptyRootOffset)
        {
            return results;
        }

        if (loEncoded > hiEncoded)
        {
            return results;
        }

        var pageOffset = NavigateToLeaf(loEncoded);
        if (pageOffset < 0)
        {
            return results;
        }

        while (true)
        {
            ReadPage(pageOffset);

            if (_pageBuf[Db2IndexFileFormat.LeafTypeOffset] != Db2IndexFileFormat.PageTypeLeaf)
            {
                break;
            }

            var entryCount = Db2IndexFileFormat.ReadUInt16(_pageBuf, Db2IndexFileFormat.LeafEntryCountOffset);
            var nextPageOffset = Db2IndexFileFormat.ReadInt64(_pageBuf, Db2IndexFileFormat.LeafNextPageOffset);
            var entrySize = Db2IndexFileFormat.GetEntrySize(_valueByteWidth);
            var pos = Db2IndexFileFormat.LeafEntriesOffset;

            for (var i = 0; i < entryCount; i++)
            {
                var key = Db2IndexFileFormat.ReadKey(_pageBuf, pos, _valueByteWidth);
                if (key >= loEncoded && key <= hiEncoded)
                {
                    var sectionIndex = Db2IndexFileFormat.ReadUInt16(_pageBuf, pos + _valueByteWidth);
                    var rowIndex = Db2IndexFileFormat.ReadInt32(_pageBuf, pos + _valueByteWidth + 2);
                    results.Add(new RowHandle(sectionIndex, rowIndex, rowId: 0));
                }
                else if (key > hiEncoded)
                {
                    return results;
                }

                pos += entrySize;
            }

            if (nextPageOffset == 0)
            {
                break;
            }

            pageOffset = nextPageOffset;
        }

        return results;
    }

    private long NavigateToLeaf(ulong encodedTarget)
    {
        var pageOffset = _rootPageOffset;

        while (true)
        {
            ReadPage(pageOffset);

            if (_pageBuf[0] == Db2IndexFileFormat.PageTypeLeaf)
            {
                return pageOffset;
            }

            if (_pageBuf[0] != Db2IndexFileFormat.PageTypeInternal)
            {
                return -1;
            }

            var keyCount = Db2IndexFileFormat.ReadUInt16(_pageBuf, Db2IndexFileFormat.InternalKeyCountOffset);
            var childIndex = BinarySearchFirstGreaterOrEqual(_pageBuf, Db2IndexFileFormat.InternalKeysOffset, keyCount, _valueByteWidth, encodedTarget);
            var childrenOffset = Db2IndexFileFormat.InternalKeysOffset + keyCount * _valueByteWidth;
            pageOffset = Db2IndexFileFormat.ReadInt64(_pageBuf, childrenOffset + childIndex * 8);
        }
    }

    private static int BinarySearchFirstGreaterOrEqual(byte[] buf, int keysOffset, int keyCount, byte byteWidth, ulong target)
    {
        int lo = 0, hi = keyCount - 1;
        while (lo <= hi)
        {
            var mid = (lo + hi) >>> 1;
            var midKey = Db2IndexFileFormat.ReadKey(buf, keysOffset + mid * byteWidth, byteWidth);
            if (midKey < target)
            {
                lo = mid + 1;
            }
            else
            {
                hi = mid - 1;
            }
        }

        return lo;
    }

    private void ReadPage(long pageOffset)
    {
        _accessor.ReadArray(pageOffset, _pageBuf, 0, Db2IndexFileFormat.PageSize);
    }

    public void Dispose()
    {
        if (_disposed)
        {
            return;
        }

        _disposed = true;
        _accessor.Dispose();
        _mmf.Dispose();
    }
}
