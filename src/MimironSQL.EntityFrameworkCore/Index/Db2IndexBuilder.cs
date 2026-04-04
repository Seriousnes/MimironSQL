using System.Threading;

using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;

using MimironSQL.Db2;
using MimironSQL.EntityFrameworkCore.Schema;
using MimironSQL.EntityFrameworkCore.Storage;
using MimironSQL.Formats;
using MimironSQL.Formats.Wdc5;
using MimironSQL.Formats.Wdc5.Db2;
using MimironSQL.Formats.Wdc5.Index;

namespace MimironSQL.EntityFrameworkCore.Index;

internal sealed class Db2IndexBuilder
{
    private readonly Db2IndexCacheLocator _cacheLocator;
    private readonly string _wowVersion;

    private int _buildState;

    public Db2IndexBuilder(Db2IndexCacheLocator cacheLocator, string wowVersion)
    {
        ArgumentNullException.ThrowIfNull(cacheLocator);
        ArgumentException.ThrowIfNullOrWhiteSpace(wowVersion);
        _cacheLocator = cacheLocator;
        _wowVersion = wowVersion;
    }

    public void EnsureBuilt(IModel efModel, IMimironDb2Store store, IDb2Format format)
    {
        if (Volatile.Read(ref _buildState) == 2)
        {
            return;
        }

        if (Interlocked.CompareExchange(ref _buildState, 1, 0) != 0)
        {
            var sw = new SpinWait();
            while (Volatile.Read(ref _buildState) == 1)
            {
                sw.SpinOnce();
            }

            return;
        }

        try
        {
            BuildAll(efModel, store, format);
            Volatile.Write(ref _buildState, 2);
        }
        catch
        {
            Volatile.Write(ref _buildState, 0);
            throw;
        }
    }

    private void BuildAll(IModel efModel, IMimironDb2Store store, IDb2Format format)
    {
        foreach (var entityType in efModel.GetEntityTypes())
        {
            if (entityType.IsOwned() || entityType.FindPrimaryKey() is null)
            {
                continue;
            }

            var tableName = entityType.GetTableName();
            if (string.IsNullOrWhiteSpace(tableName))
            {
                continue;
            }

            Db2TableSchema schema;
            try
            {
                schema = store.GetSchema(tableName);
            }
            catch
            {
                continue; // Table not found or not accessible — skip.
            }

            var indexableFields = schema.Fields
                .Where(f => !f.IsId && !f.IsVirtual && !f.IsRelation
                    && f.ValueType is Db2ValueType.Int64 or Db2ValueType.UInt64 or Db2ValueType.Single)
                .ToList();

            if (indexableFields.Count == 0)
            {
                continue;
            }

            IDb2File file;
            uint layoutHash;

            try
            {
                var openedTable = store.OpenTableWithSchema(tableName);
                file = openedTable.File;
                layoutHash = format.GetLayout(file).LayoutHash;
            }
            catch
            {
                continue;
            }

            foreach (var field in indexableFields)
            {
                var indexFilePath = _cacheLocator.GetIndexFilePath(_wowVersion, tableName, field.Name, layoutHash);

                if (IsIndexValid(indexFilePath, _wowVersion, layoutHash))
                {
                    continue;
                }

                try
                {
                    BuildIndexFile(file, field, tableName, _wowVersion, layoutHash, indexFilePath);
                }
                catch
                {
                }
            }
        }
    }

    private static void BuildIndexFile(
        IDb2File file,
        Db2FieldSchema field,
        string tableName,
        string wowVersion,
        uint layoutHash,
        string filePath)
    {
        var byteWidth = GetValueByteWidth(file, field.ColumnStartIndex);

        var entries = new List<(ulong EncodedValue, ushort SectionIndex, int RowIndex)>(capacity: file.RecordsCount);

        foreach (var handle in file.EnumerateRowHandles())
        {
            var encodedValue = ReadEncodedValue(file, handle, field.ColumnStartIndex, field.ValueType, byteWidth);
            entries.Add((encodedValue, (ushort)handle.SectionIndex, handle.RowIndexInSection));
        }

        entries.Sort(static (a, b) =>
        {
            var c = a.EncodedValue.CompareTo(b.EncodedValue);
            if (c != 0)
            {
                return c;
            }

            c = a.SectionIndex.CompareTo(b.SectionIndex);
            return c != 0 ? c : a.RowIndex.CompareTo(b.RowIndex);
        });

        Db2IndexWriter.Write(
            filePath: filePath,
            sortedEntries: entries,
            valueByteWidth: byteWidth,
            tableName: tableName,
            fieldIndex: field.ColumnStartIndex,
            valueType: (byte)field.ValueType,
            wowVersion: wowVersion,
            layoutHash: layoutHash);
    }

    internal static byte GetValueByteWidth(IDb2File file, int fieldIndex)
    {
        if (file is not Wdc5File wdc5)
        {
            return 4;
        }

        var fieldMeta = wdc5.FieldMeta[fieldIndex];
        var columnMeta = wdc5.ColumnMeta[fieldIndex];

        int bitWidth = columnMeta.CompressionType switch
        {
            CompressionType.Common
            or CompressionType.Pallet
            or CompressionType.PalletArray => 32,

            CompressionType.None =>
                32 - fieldMeta.Bits is > 0 and int fromBits ? fromBits : columnMeta.Immediate.BitWidth,

            _ => columnMeta.Immediate.BitWidth,
        };

        if (bitWidth <= 0)
        {
            bitWidth = 32;
        }

        return bitWidth switch
        {
            <= 8 => 1,
            <= 16 => 2,
            <= 32 => 4,
            _ => 8,
        };
    }

    private static ulong ReadEncodedValue(IDb2File file, RowHandle handle, int fieldIndex, Db2ValueType valueType, byte byteWidth)
    {
        return (valueType, byteWidth) switch
        {
            (Db2ValueType.Single, _) => Db2IndexValueEncoder.Encode(file.ReadField<float>(handle, fieldIndex)),
            (Db2ValueType.Int64, 1) => Db2IndexValueEncoder.Encode(file.ReadField<sbyte>(handle, fieldIndex)),
            (Db2ValueType.Int64, 2) => Db2IndexValueEncoder.Encode(file.ReadField<short>(handle, fieldIndex)),
            (Db2ValueType.Int64, 4) => Db2IndexValueEncoder.Encode(file.ReadField<int>(handle, fieldIndex)),
            (Db2ValueType.Int64, 8) => Db2IndexValueEncoder.Encode(file.ReadField<long>(handle, fieldIndex)),
            (Db2ValueType.UInt64, 1) => Db2IndexValueEncoder.Encode(file.ReadField<byte>(handle, fieldIndex)),
            (Db2ValueType.UInt64, 2) => Db2IndexValueEncoder.Encode(file.ReadField<ushort>(handle, fieldIndex)),
            (Db2ValueType.UInt64, 4) => Db2IndexValueEncoder.Encode(file.ReadField<uint>(handle, fieldIndex)),
            (Db2ValueType.UInt64, 8) => Db2IndexValueEncoder.Encode(file.ReadField<ulong>(handle, fieldIndex)),
            _ => Db2IndexValueEncoder.Encode(file.ReadField<uint>(handle, fieldIndex)),
        };
    }

    internal static bool IsIndexValid(string filePath, string wowVersion, uint layoutHash)
    {
        if (!File.Exists(filePath))
        {
            return false;
        }

        try
        {
            using var stream = File.OpenRead(filePath);
            if (stream.Length < Db2IndexFileFormat.PageSize)
            {
                return false;
            }

            var header = new byte[Db2IndexFileFormat.PageSize];
            stream.ReadExactly(header);

            var magic = Db2IndexFileFormat.ReadUInt32(header, Db2IndexFileFormat.HdrMagicOffset);
            if (magic != Db2IndexFileFormat.Magic)
            {
                return false;
            }

            var version = Db2IndexFileFormat.ReadInt32(header, Db2IndexFileFormat.HdrVersionOffset);
            if (version != Db2IndexFileFormat.FormatVersion)
            {
                return false;
            }

            var storedHash = Db2IndexFileFormat.ReadUInt32(header, Db2IndexFileFormat.HdrLayoutHashOffset);
            if (storedHash != layoutHash)
            {
                return false;
            }

            var storedVersion = Db2IndexFileFormat.ReadFixedLengthString(header, Db2IndexFileFormat.HdrWowVersionOffset, Db2IndexFileFormat.WowVersionMaxBytes);
            if (!string.IsNullOrEmpty(storedVersion) && storedVersion != wowVersion)
            {
                return false;
            }

            return true;
        }
        catch
        {
            return false;
        }
    }
}
