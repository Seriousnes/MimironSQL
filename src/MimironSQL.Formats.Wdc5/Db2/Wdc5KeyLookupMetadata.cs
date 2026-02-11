using System.Buffers;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;

using MimironSQL.Db2;

namespace MimironSQL.Formats.Wdc5.Db2;

internal sealed class Wdc5KeyLookupMetadata
{
    private const int HeaderSize = 200;
    private const uint Wdc5Magic = 0x35434457; // "WDC5"

    public Wdc5Header Header { get; }

    public IReadOnlyList<Wdc5SectionHeader> Sections { get; }

    public IReadOnlyList<Section> ParsedSections { get; }

    public FieldMetaData[] FieldMeta { get; }

    public ColumnMetaData[] ColumnMeta { get; }

    public uint[][] PalletData { get; }

    public Dictionary<int, uint>[] CommonData { get; }

    public int RecordsBlobSizeBytes { get; }

    private readonly Dictionary<int, (int SectionIndex, int RowIndexInSection, int GlobalRowIndex)> _idIndex;
    private readonly Dictionary<int, int> _copyMap;

    private Wdc5KeyLookupMetadata(
        Wdc5Header header,
        IReadOnlyList<Wdc5SectionHeader> sections,
        IReadOnlyList<Section> parsedSections,
        FieldMetaData[] fieldMeta,
        ColumnMetaData[] columnMeta,
        uint[][] palletData,
        Dictionary<int, uint>[] commonData,
        int recordsBlobSizeBytes,
        Dictionary<int, (int SectionIndex, int RowIndexInSection, int GlobalRowIndex)> idIndex,
        Dictionary<int, int> copyMap)
    {
        Header = header;
        Sections = sections;
        ParsedSections = parsedSections;
        FieldMeta = fieldMeta;
        ColumnMeta = columnMeta;
        PalletData = palletData;
        CommonData = commonData;
        RecordsBlobSizeBytes = recordsBlobSizeBytes;
        _idIndex = idIndex;
        _copyMap = copyMap;
    }

    public bool TryResolveRowHandle(int requestedId, out RowHandle handle, out RowResolution resolution)
    {
        var key = requestedId;

        if (_copyMap.TryGetValue(key, out var sourceId))
            key = sourceId;

        if (!_idIndex.TryGetValue(key, out var location))
        {
            handle = default;
            resolution = default;
            return false;
        }

        handle = new RowHandle(location.SectionIndex, location.RowIndexInSection, requestedId);

        var section = ParsedSections[location.SectionIndex];

        // For non-copy rows, requestedId == sourceId.
        var resolvedSourceId = key;

        var referenceKey = Header.Flags.HasFlag(Db2Flags.SecondaryKey) && section.IndexData is { Length: not 0 }
            ? section.IndexData[location.RowIndexInSection]
            : location.RowIndexInSection;

        _ = section.ParentLookupEntries.TryGetValue(referenceKey, out var parentRelationId);

        resolution = new RowResolution(
            SectionIndex: location.SectionIndex,
            RowIndexInSection: location.RowIndexInSection,
            GlobalRowIndex: location.GlobalRowIndex,
            SourceId: resolvedSourceId,
            DestinationId: requestedId,
            ParentRelationId: parentRelationId);

        return true;
    }

    public Section GetSection(int sectionIndex) => ParsedSections[sectionIndex];

    public static Wdc5KeyLookupMetadata Parse(Stream stream, Wdc5FileOptions? options)
    {
        ArgumentNullException.ThrowIfNull(stream);
        if (!stream.CanSeek)
            throw new NotSupportedException("WDC5 key lookup requires a seekable Stream.");

        options ??= new Wdc5FileOptions();

        stream.Position = 0;
        using var reader = new BinaryReader(stream, Encoding.UTF8, leaveOpen: true);

        if (reader.BaseStream.Length is < HeaderSize)
            throw new InvalidDataException("DB2 file is too small to be valid.");

        var magic = reader.ReadUInt32();
        if (magic is not Wdc5Magic)
            throw new InvalidDataException("Expected WDC5.");

        var schemaVersion = reader.ReadUInt32();
        var schemaString = Encoding.UTF8.GetString(reader.ReadBytes(128)).TrimEnd('\0');
        var recordsCount = reader.ReadInt32();
        var fieldsCount = reader.ReadInt32();
        var recordSize = reader.ReadInt32();
        var stringTableSize = reader.ReadInt32();
        var tableHash = reader.ReadUInt32();
        var layoutHash = reader.ReadUInt32();
        var minIndex = reader.ReadInt32();
        var maxIndex = reader.ReadInt32();
        var locale = reader.ReadInt32();
        var flags = (Db2Flags)reader.ReadUInt16();
        var idFieldIndex = reader.ReadUInt16();
        var totalFieldsCount = reader.ReadInt32();
        var packedDataOffset = reader.ReadInt32();
        var lookupColumnCount = reader.ReadInt32();
        var columnMetaDataSize = reader.ReadInt32();
        var commonDataSize = reader.ReadInt32();
        var palletDataSize = reader.ReadInt32();
        var sectionsCount = reader.ReadInt32();

        var header = new Wdc5Header(
            schemaVersion,
            schemaString,
            recordsCount,
            fieldsCount,
            recordSize,
            stringTableSize,
            tableHash,
            layoutHash,
            minIndex,
            maxIndex,
            locale,
            flags,
            idFieldIndex,
            totalFieldsCount,
            packedDataOffset,
            lookupColumnCount,
            columnMetaDataSize,
            commonDataSize,
            palletDataSize,
            sectionsCount);

        var sections = new List<Wdc5SectionHeader>(Math.Max(0, sectionsCount));
        for (var i = 0; i < sectionsCount; i++)
        {
            var tactKeyLookup = reader.ReadUInt64();
            var fileOffset = reader.ReadInt32();
            var numRecords = reader.ReadInt32();
            var sectionStringTableSize = reader.ReadInt32();
            var offsetRecordsEndOffset = reader.ReadInt32();
            var indexDataSize = reader.ReadInt32();
            var parentLookupDataSize = reader.ReadInt32();
            var offsetMapIdCount = reader.ReadInt32();
            var copyTableCount = reader.ReadInt32();
            sections.Add(new Wdc5SectionHeader(
                tactKeyLookup,
                fileOffset,
                numRecords,
                sectionStringTableSize,
                offsetRecordsEndOffset,
                indexDataSize,
                parentLookupDataSize,
                offsetMapIdCount,
                copyTableCount));
        }

        var fieldMeta = new FieldMetaData[fieldsCount];
        for (var i = 0; i < fieldsCount; i++)
            fieldMeta[i] = new FieldMetaData(reader.ReadInt16(), reader.ReadInt16());

        var columnMeta = new ColumnMetaData[fieldsCount];
        var metaBytes = reader.ReadBytes(Unsafe.SizeOf<ColumnMetaData>() * fieldsCount);
        MemoryMarshal.Cast<byte, ColumnMetaData>(metaBytes).CopyTo(columnMeta);

        var palletData = new uint[columnMeta.Length][];
        for (var i = 0; i < columnMeta.Length; i++)
        {
            switch (columnMeta[i].CompressionType)
            {
                case CompressionType.Pallet or CompressionType.PalletArray:
                    {
                        var count = checked((int)columnMeta[i].AdditionalDataSize / 4);
                        var bytes = reader.ReadBytes(count * 4);
                        var values = new uint[count];
                        MemoryMarshal.Cast<byte, uint>(bytes).CopyTo(values);
                        palletData[i] = values;
                        break;
                    }

                default:
                    palletData[i] = [];
                    break;
            }
        }

        var commonData = new Dictionary<int, uint>[columnMeta.Length];
        for (var i = 0; i < columnMeta.Length; i++)
        {
            switch (columnMeta[i])
            {
                case { CompressionType: CompressionType.Common }:
                    {
                        var count = checked((int)columnMeta[i].AdditionalDataSize / 8);
                        var dict = new Dictionary<int, uint>(count);
                        for (var j = 0; j < count; j++)
                        {
                            var id = reader.ReadInt32();
                            var value = reader.ReadUInt32();
                            dict[id] = value;
                        }
                        commonData[i] = dict;
                        break;
                    }

                default:
                    commonData[i] = [];
                    break;
            }
        }

        var parsedSections = new List<Section>(sections.Count);
        var idIndex = new Dictionary<int, (int SectionIndex, int RowIndexInSection, int GlobalRowIndex)>(capacity: Math.Max(0, recordsCount));
        var copyMap = new Dictionary<int, int>();

        var previousRecordCount = 0;
        var previousStringTableSize = 0;
        var previousRecordBlobSizeBytes = 0;

        foreach (var sectionHeader in sections)
        {
            reader.BaseStream.Position = sectionHeader.FileOffset;

            ReadOnlyMemory<byte> tactKey = ReadOnlyMemory<byte>.Empty;
            if (sectionHeader is { TactKeyLookup: not 0 } && options.TactKeyProvider is not null)
                _ = options.TactKeyProvider.TryGetKey(sectionHeader.TactKeyLookup, out tactKey);

            var isSparse = flags.HasFlag(Db2Flags.Sparse);

            var recordDataSizeBytes = !isSparse
                ? checked(sectionHeader.NumRecords * recordSize)
                : sectionHeader.OffsetRecordsEndOffset - sectionHeader.FileOffset;

            if (!isSparse)
            {
                reader.BaseStream.Position += recordDataSizeBytes;

                if (sectionHeader.StringTableSize > 0)
                    reader.BaseStream.Position += sectionHeader.StringTableSize;
            }
            else
            {
                if (reader.BaseStream.Position != sectionHeader.FileOffset)
                    throw new InvalidDataException("Unexpected stream position for sparse section.");

                reader.BaseStream.Position += recordDataSizeBytes;

                if (reader.BaseStream.Position != sectionHeader.OffsetRecordsEndOffset)
                    throw new InvalidDataException("WDC5 sparse section parsing desynced: expected OffsetRecordsEndOffset.");
            }

            var isEncrypted = sectionHeader is { TactKeyLookup: not 0 };
            var shouldSkipEncryptedSection = isEncrypted && tactKey.IsEmpty;

            // index data
            var indexData = ReadInt32Array(reader, sectionHeader.IndexDataSize / 4);
            if (indexData is { Length: > 0 } && indexData.All(x => x == 0))
                indexData = [.. Enumerable.Range(minIndex + previousRecordCount, sectionHeader.NumRecords)];

            // copy table
            var copyData = new Dictionary<int, int>();
            if (sectionHeader is { CopyTableCount: > 0 })
            {
                for (var i = 0; i < sectionHeader.CopyTableCount; i++)
                {
                    var destinationRowId = reader.ReadInt32();
                    var sourceRowId = reader.ReadInt32();
                    if (destinationRowId != sourceRowId)
                    {
                        copyData[destinationRowId] = sourceRowId;
                        copyMap[destinationRowId] = sourceRowId;
                    }
                }
            }

            // offset map / sparse entries
            SparseEntry[] sparseEntries = [];
            if (sectionHeader is { OffsetMapIDCount: > 0 })
                sparseEntries = ReadStructArray<SparseEntry>(reader, sectionHeader.OffsetMapIDCount);

            // secondary key sparse index data
            if (sectionHeader is { OffsetMapIDCount: > 0 } && flags.HasFlag(Db2Flags.SecondaryKey))
            {
                var sparseIndexData = ReadInt32Array(reader, sectionHeader.OffsetMapIDCount);
                if (sectionHeader is { IndexDataSize: > 0 } && indexData.Length != sparseIndexData.Length)
                    throw new InvalidDataException("WDC5 sparse index data length mismatch.");
                indexData = sparseIndexData;
            }

            // parent lookup data
            var parentLookupEntries = new Dictionary<int, int>();
            if (sectionHeader is { ParentLookupDataSize: > 0 })
            {
                var numRecords = reader.ReadInt32();
                _ = reader.ReadInt32(); // minId
                _ = reader.ReadInt32(); // maxId

                for (var i = 0; i < numRecords; i++)
                {
                    var index = reader.ReadInt32();
                    var id = reader.ReadInt32();
                    parentLookupEntries[index] = id;
                }
            }

            if (sectionHeader is { OffsetMapIDCount: > 0 } && !flags.HasFlag(Db2Flags.SecondaryKey))
            {
                var sparseIndexData = ReadInt32Array(reader, sectionHeader.OffsetMapIDCount);
                if (sectionHeader is { IndexDataSize: > 0 } && indexData.Length != sparseIndexData.Length)
                    throw new InvalidDataException("WDC5 sparse index data length mismatch.");
                indexData = sparseIndexData;
            }

            var sparseStarts = flags.HasFlag(Db2Flags.Sparse)
                ? Wdc5Section.BuildSparseRecordStartBits(sparseEntries, sectionHeader.FileOffset, recordDataSizeBytes)
                : [];

            if (!shouldSkipEncryptedSection)
            {
                var parsed = new Section
                {
                    Header = sectionHeader,
                    FirstGlobalRecordIndex = previousRecordCount,
                    RecordDataSizeBytes = recordDataSizeBytes,
                    RecordsBaseOffsetInBlob = previousRecordBlobSizeBytes,
                    StringTableBaseOffset = previousStringTableSize,
                    IndexData = indexData,
                    CopyData = copyData,
                    ParentLookupEntries = parentLookupEntries,
                    SparseEntries = sparseEntries,
                    SparseRecordStartBits = sparseStarts,
                    TactKey = tactKey,
                };

                var sectionIndex = parsedSections.Count;
                parsedSections.Add(parsed);

                // Build the source-id index from index data.
                if (indexData is { Length: not 0 })
                {
                    for (var rowIndex = 0; rowIndex < parsed.Header.NumRecords; rowIndex++)
                    {
                        var id = indexData[rowIndex];
                        if (id != -1)
                            idIndex.TryAdd(id, (sectionIndex, rowIndex, parsed.FirstGlobalRecordIndex + rowIndex));
                    }
                }
            }

            previousRecordCount += sectionHeader.NumRecords;
            previousStringTableSize += sectionHeader.StringTableSize;
            previousRecordBlobSizeBytes += recordDataSizeBytes;
        }

        if (recordsCount > 0 && parsedSections is { Count: 0 })
            throw new NotSupportedException("All WDC5 sections are encrypted or unreadable (missing TACT keys or placeholder section data).");

        return new Wdc5KeyLookupMetadata(
            header,
            sections,
            parsedSections,
            fieldMeta,
            columnMeta,
            palletData,
            commonData,
            recordsBlobSizeBytes: previousRecordBlobSizeBytes,
            idIndex,
            copyMap);
    }

    private static T[] ReadStructArray<T>(BinaryReader reader, int count) where T : unmanaged
    {
        if (count <= 0)
            return [];

        var size = Unsafe.SizeOf<T>();
        var bytes = reader.ReadBytes(count * size);
        return [.. MemoryMarshal.Cast<byte, T>(bytes)];
    }

    private static int[] ReadInt32Array(BinaryReader reader, int count)
    {
        if (count <= 0)
            return [];

        var bytes = reader.ReadBytes(count * 4);
        return [.. MemoryMarshal.Cast<byte, int>(bytes)];
    }

    internal readonly record struct RowResolution(
        int SectionIndex,
        int RowIndexInSection,
        int GlobalRowIndex,
        int SourceId,
        int DestinationId,
        int ParentRelationId);

    internal sealed class Section
    {
        public required Wdc5SectionHeader Header { get; init; }

        public required int FirstGlobalRecordIndex { get; init; }

        public required int RecordDataSizeBytes { get; init; }

        public required int RecordsBaseOffsetInBlob { get; init; }

        public required int StringTableBaseOffset { get; init; }

        public ReadOnlyMemory<byte> TactKey { get; init; } = ReadOnlyMemory<byte>.Empty;

        public int[] IndexData { get; init; } = [];

        public Dictionary<int, int> CopyData { get; init; } = [];

        public Dictionary<int, int> ParentLookupEntries { get; init; } = [];

        public SparseEntry[] SparseEntries { get; init; } = [];

        public int[] SparseRecordStartBits { get; init; } = [];

        public bool IsEncrypted => Header is { TactKeyLookup: not 0 };

        public bool IsDecryptable => IsEncrypted && !TactKey.IsEmpty;

        public int NumRecords => Header.NumRecords;
    }
}
