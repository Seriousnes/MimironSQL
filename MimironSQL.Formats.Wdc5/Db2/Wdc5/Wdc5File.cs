using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;

using MimironSQL.Db2;
using MimironSQL.Formats;

namespace MimironSQL.Formats.Wdc5;

public sealed class Wdc5File : IDb2File<Wdc5Row>, IDb2DenseStringTableIndexProvider<Wdc5Row>
{
    private const int HeaderSize = 200;
    private const uint Wdc5Magic = 0x35434457; // "WDC5"

    public Wdc5Header Header { get; }
    public IReadOnlyList<Wdc5SectionHeader> Sections { get; }
    public IReadOnlyList<Wdc5Section> ParsedSections { get; }
    public FieldMetaData[] FieldMeta { get; }
    public ColumnMetaData[] ColumnMeta { get; }
    public Value32[][] PalletData { get; }
    public Dictionary<int, Value32>[] CommonData { get; }

    public ReadOnlyMemory<byte> DenseStringTableBytes { get; } = ReadOnlyMemory<byte>.Empty;
    public int RecordsBlobSizeBytes { get; }

    public Type RowType => typeof(Wdc5Row);

    public Db2Flags Flags => Header.Flags;

    public int RecordsCount => Header.RecordsCount;

    public int TotalSectionRecordCount => ParsedSections.Sum(s => s.NumRecords);

    private Dictionary<int, (int SectionIndex, int RowIndexInSection, int GlobalRecordIndex)>? _idIndex;
    private Dictionary<int, int>? _copyMap;

    internal Wdc5FileOptions Options { get; }

    public Wdc5File(Stream stream) : this(stream, options: null)
    {
    }

    public Wdc5File(Stream stream, Wdc5FileOptions? options)
    {
        ArgumentNullException.ThrowIfNull(stream);
        if (!stream.CanSeek)
            throw new NotSupportedException("WDC5 parsing requires a seekable Stream.");

        Options = options ?? new Wdc5FileOptions();

        stream.Position = 0;
        using var reader = new BinaryReader(stream, Encoding.UTF8, leaveOpen: true);

        if (reader.BaseStream.Length is < HeaderSize)
            throw new InvalidDataException("DB2 file is too small to be valid.");

        var magic = reader.ReadUInt32();
        if (magic is not Wdc5Magic)
        {
            Span<byte> headerBytes = stackalloc byte[4];
            Unsafe.WriteUnaligned(ref headerBytes[0], magic);
            var detected = Db2FormatDetector.Detect(headerBytes);
            throw new InvalidDataException($"Expected WDC5 but found {detected}.");
        }

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
            // matches DBCD.IO.Common.SectionHeaderWDC5 (Pack=2)
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

        var palletData = new Value32[columnMeta.Length][];
        for (var i = 0; i < columnMeta.Length; i++)
        {
            if (columnMeta[i].CompressionType is CompressionType.Pallet or CompressionType.PalletArray)
            {
                var count = checked((int)columnMeta[i].AdditionalDataSize / 4);
                var bytes = reader.ReadBytes(count * 4);
                var values = new Value32[count];
                MemoryMarshal.Cast<byte, Value32>(bytes).CopyTo(values);
                palletData[i] = values;
            }
            else
            {
                palletData[i] = [];
            }
        }

        var commonData = new Dictionary<int, Value32>[columnMeta.Length];
        for (var i = 0; i < columnMeta.Length; i++)
        {
            if (columnMeta[i] is { CompressionType: CompressionType.Common })
            {
                var count = checked((int)columnMeta[i].AdditionalDataSize / 8);
                var dict = new Dictionary<int, Value32>(count);
                for (var j = 0; j < count; j++)
                {
                    var id = reader.ReadInt32();
                    var valueBytes = reader.ReadBytes(4);
                    dict[id] = MemoryMarshal.Read<Value32>(valueBytes);
                }
                commonData[i] = dict;
            }
            else
            {
                commonData[i] = new Dictionary<int, Value32>();
            }
        }

        var parsedSections = new List<Wdc5Section>(sections.Count);
        var denseStringTableBytes = new List<byte>(Math.Max(0, stringTableSize));
        var recordsBlobSizeBytes = 0;

        if (sectionsCount != 0 && recordsCount != 0)
        {
            var previousRecordCount = 0;
            var previousStringTableSize = 0;
            var previousRecordBlobSizeBytes = 0;
            foreach (var section in sections)
            {
                reader.BaseStream.Position = section.FileOffset;

                ReadOnlyMemory<byte> tactKey = ReadOnlyMemory<byte>.Empty;
                if (section is { TactKeyLookup: not 0 } && Options.TactKeyProvider is not null)
                    _ = Options.TactKeyProvider.TryGetKey(section.TactKeyLookup, out tactKey);

                byte[] recordsData;
                byte[] stringTableBytes;
                int recordDataSizeBytes;
                if (!flags.HasFlag(Db2Flags.Sparse))
                {
                    recordDataSizeBytes = section.NumRecords * recordSize;
                    recordsData = reader.ReadBytes(recordDataSizeBytes);
                    Array.Resize(ref recordsData, recordsData.Length + 8);
                    stringTableBytes = reader.ReadBytes(section.StringTableSize);

                    denseStringTableBytes.AddRange(stringTableBytes);
                }
                else
                {
                    recordDataSizeBytes = section.OffsetRecordsEndOffset - section.FileOffset;
                    recordsData = reader.ReadBytes(recordDataSizeBytes);
                    stringTableBytes = [];

                    if (reader.BaseStream.Position != section.OffsetRecordsEndOffset)
                        throw new InvalidDataException("WDC5 sparse section parsing desynced: expected OffsetRecordsEndOffset.");
                }

                var isEncrypted = section is { TactKeyLookup: not 0 };
                var shouldSkipEncryptedSection = false;
                if (isEncrypted)
                {
                    if (tactKey.IsEmpty)
                    {
                        shouldSkipEncryptedSection = true;
                    }
                    else
                    {
                        var allZero = true;
                        for (var i = 0; i < recordDataSizeBytes; i++)
                        {
                            if (recordsData[i] != 0)
                            {
                                allZero = false;
                                break;
                            }
                        }

                        // Some encrypted sections may be present as placeholders (all zero-filled). Skip for now.
                        if (allZero)
                            shouldSkipEncryptedSection = true;
                    }
                }

                // index data
                var indexData = ReadInt32Array(reader, section.IndexDataSize / 4);
                if (indexData is { Length: > 0 } && indexData.All(x => x == 0))
                    indexData = [.. Enumerable.Range(minIndex + previousRecordCount, section.NumRecords)];

                // copy table
                var copyData = new Dictionary<int, int>();
                if (section is { CopyTableCount: > 0 })
                {
                    for (var i = 0; i < section.CopyTableCount; i++)
                    {
                        var destinationRowId = reader.ReadInt32();
                        var sourceRowId = reader.ReadInt32();
                        if (destinationRowId != sourceRowId)
                            copyData[destinationRowId] = sourceRowId;
                    }
                }

                // offset map / sparse entries
                SparseEntry[] sparseEntries = [];
                if (section is { OffsetMapIDCount: > 0 })
                    sparseEntries = ReadStructArray<SparseEntry>(reader, section.OffsetMapIDCount);

                // secondary key sparse index data (not fully surfaced yet)
                if (section is { OffsetMapIDCount: > 0 } && flags.HasFlag(Db2Flags.SecondaryKey))
                {
                    var sparseIndexData = ReadInt32Array(reader, section.OffsetMapIDCount);
                    if (section is { IndexDataSize: > 0 } && indexData.Length != sparseIndexData.Length)
                        throw new InvalidDataException("WDC5 sparse index data length mismatch.");
                    indexData = sparseIndexData;
                }

                // parent lookup data (parsed only enough to advance stream)
                var parentLookupEntries = new Dictionary<int, int>();
                if (section is { ParentLookupDataSize: > 0 })
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

                // if OffsetMap exists but we didn't read sparse index earlier, WDC5 can have it here too
                if (section is { OffsetMapIDCount: > 0 } && !flags.HasFlag(Db2Flags.SecondaryKey))
                {
                    var sparseIndexData = ReadInt32Array(reader, section.OffsetMapIDCount);
                    if (section is { IndexDataSize: > 0 } && indexData.Length != sparseIndexData.Length)
                        throw new InvalidDataException("WDC5 sparse index data length mismatch.");
                    indexData = sparseIndexData;
                }

                var sparseStarts = flags.HasFlag(Db2Flags.Sparse)
                    ? Wdc5Section.BuildSparseRecordStartBits(sparseEntries, section.FileOffset, recordDataSizeBytes)
                    : [];

                if (!shouldSkipEncryptedSection)
                {
                    parsedSections.Add(new Wdc5Section
                    {
                        Header = section,
                        FirstGlobalRecordIndex = previousRecordCount,
                        RecordsData = recordsData,
                        RecordDataSizeBytes = recordDataSizeBytes,
                        RecordsBaseOffsetInBlob = previousRecordBlobSizeBytes,
                        StringTableBaseOffset = previousStringTableSize,
                        StringTableBytes = stringTableBytes,
                        IndexData = indexData,
                        CopyData = copyData,
                        ParentLookupEntries = parentLookupEntries,
                        SparseEntries = sparseEntries,
                        SparseRecordStartBits = sparseStarts,
                        TactKey = tactKey,
                    });
                }

                previousRecordCount += section.NumRecords;
                previousStringTableSize += section.StringTableSize;
                previousRecordBlobSizeBytes += recordDataSizeBytes;
            }

            if (previousRecordBlobSizeBytes < 0)
                throw new InvalidDataException("WDC5 record blob size overflow.");

            recordsBlobSizeBytes = previousRecordBlobSizeBytes;
        }

        Header = header;
        Sections = sections;
        ParsedSections = parsedSections;
        FieldMeta = fieldMeta;
        ColumnMeta = columnMeta;
        PalletData = palletData;
        CommonData = commonData;
        DenseStringTableBytes = new ReadOnlyMemory<byte>([.. denseStringTableBytes]);
        RecordsBlobSizeBytes = recordsBlobSizeBytes;

        if (recordsCount > 0 && parsedSections is { Count: 0 })
            throw new NotSupportedException("All WDC5 sections are encrypted or unreadable (missing TACT keys or placeholder section data).");
    }

    public IEnumerable<Wdc5Row> EnumerateRows()
    {
        var globalIndex = 0;
        for (var sectionIndex = 0; sectionIndex < ParsedSections.Count; sectionIndex++)
        {
            var section = ParsedSections[sectionIndex];
            for (var rowIndex = 0; rowIndex < section.NumRecords; rowIndex++)
            {
                var reader = CreateReaderAtRowStart(section, rowIndex);
                var id = GetVirtualId(section, rowIndex, reader);
                var referenceKey = Header.Flags.HasFlag(Db2Flags.SecondaryKey) && section.IndexData is { Length: not 0 }
                    ? section.IndexData[rowIndex]
                    : rowIndex;

                _ = section.ParentLookupEntries.TryGetValue(referenceKey, out var parentRelationId);
                yield return new Wdc5Row(this, section, reader, globalIndex, rowIndex, id, sourceId: id, parentRelationId);
                globalIndex++;
            }
        }
    }

    public bool TryGetRowById(int id, out Wdc5Row row)
    {
        Wdc5FileLookupTracker.OnTryGetRowById();

        EnsureIndexesBuilt();

        var requestedId = id;

        if (_copyMap!.TryGetValue(id, out var sourceId))
            id = sourceId;

        if (!_idIndex!.TryGetValue(id, out var location))
        {
            row = default;
            return false;
        }

        var section = ParsedSections[location.SectionIndex];
        var reader = CreateReaderAtRowStart(section, location.RowIndexInSection);

        var referenceKey = Header.Flags.HasFlag(Db2Flags.SecondaryKey) && section.IndexData is { Length: not 0 }
            ? section.IndexData[location.RowIndexInSection]
            : location.RowIndexInSection;

        _ = section.ParentLookupEntries.TryGetValue(referenceKey, out var parentRelationId);
        row = new Wdc5Row(this, section, reader, globalRowIndex: location.GlobalRecordIndex, rowIndexInSection: location.RowIndexInSection, id: requestedId, sourceId: id, parentRelationId);
        return true;
    }

    public bool TryGetDenseStringTableIndex(Wdc5Row row, int fieldIndex, out int stringTableIndex)
        => row.TryGetDenseStringTableIndex(fieldIndex, out stringTableIndex);

    public bool TryGetRowById<TId>(TId id, out Wdc5Row row) where TId : IBinaryInteger<TId>
    {
        var key = id switch
        {
            int x => x,
            uint x => unchecked((int)x),
            short x => x,
            ushort x => x,
            sbyte x => x,
            byte x => x,
            long x => checked((int)x),
            ulong x => x <= uint.MaxValue ? unchecked((int)(uint)x) : throw new OverflowException("DB2 ID is larger than 32-bit; this engine currently indexes rows by 32-bit ID."),
            nint x => checked((int)x),
            nuint x => x <= uint.MaxValue ? unchecked((int)(uint)x) : throw new OverflowException("DB2 ID is larger than 32-bit; this engine currently indexes rows by 32-bit ID."),
            _ => throw new NotSupportedException($"Unsupported ID type {typeof(TId).FullName}.")
        };

        return TryGetRowById(key, out row);
    }

    private void EnsureIndexesBuilt()
    {
        if (_idIndex is not null)
            return;

        var idIndex = new Dictionary<int, (int SectionIndex, int RowIndexInSection, int GlobalRecordIndex)>(capacity: Header.RecordsCount);
        var copyMap = new Dictionary<int, int>();

        for (var sectionIndex = 0; sectionIndex < ParsedSections.Count; sectionIndex++)
        {
            var section = ParsedSections[sectionIndex];
            foreach (var kvp in section.CopyData)
                copyMap[kvp.Key] = kvp.Value;

            for (var rowIndex = 0; rowIndex < section.NumRecords; rowIndex++)
            {
                var reader = CreateReaderAtRowStart(section, rowIndex);
                var id = GetVirtualId(section, rowIndex, reader);
                if (id != -1)
                    idIndex.TryAdd(id, (sectionIndex, rowIndex, section.FirstGlobalRecordIndex + rowIndex));
            }
        }

        _idIndex = idIndex;
        _copyMap = copyMap;
    }

    private BitReader CreateReaderAtRowStart(Wdc5Section section, int rowIndex)
    {
        var reader = new BitReader(section.RecordsData)
        {
            OffsetBytes = 0,
            PositionBits = 0,
        };

        if (!Header.Flags.HasFlag(Db2Flags.Sparse))
        {
            reader.OffsetBytes = rowIndex * Header.RecordSize;
            return reader;
        }

        if (section.SparseRecordStartBits is { Length: 0 })
            throw new InvalidDataException("Sparse WDC5 section missing SparseRecordStartBits.");

        var startBits = section.SparseRecordStartBits[rowIndex];
        var startBytes = startBits >> 3;
        var sizeBytes = (int)section.SparseEntries[rowIndex].Size;
        var endExclusive = (long)startBytes + sizeBytes;
        if (startBytes < 0 || endExclusive < 0 || endExclusive > section.RecordsData.Length)
            throw new InvalidDataException("Sparse WDC5 row points outside section record data.");

        reader.PositionBits = startBits;
        return reader;
    }

    private int GetVirtualId(Wdc5Section section, int rowIndex, BitReader readerAtStart)
    {
        // A) Prefer IndexData when present.
        if (section.IndexData is { Length: not 0 })
            return section.IndexData[rowIndex];

        // Fallback: decode the physical ID field from record bits.
        if (Header.IdFieldIndex >= Header.FieldsCount)
            return section.FirstGlobalRecordIndex + rowIndex;

        var tmp = readerAtStart;
        for (var i = 0; i <= Header.IdFieldIndex; i++)
        {
            ref readonly var fieldMeta = ref FieldMeta[i];
            ref readonly var columnMeta = ref ColumnMeta[i];
            var palletData = PalletData[i];
            var commonData = CommonData[i];

            if (columnMeta is { CompressionType: CompressionType.Common })
                throw new NotSupportedException("Decoding ID from record bits is not supported for Common-compressed ID fields.");

            if (i == Header.IdFieldIndex)
                return Wdc5FieldDecoder.ReadScalar<int>(id: 0, ref tmp, fieldMeta, columnMeta, palletData, commonData);

            _ = Wdc5FieldDecoder.ReadScalar<uint>(id: 0, ref tmp, fieldMeta, columnMeta, palletData, commonData);
        }

        return -1;
    }

    private static int[] ReadInt32Array(BinaryReader reader, int count)
    {
        if (count <= 0)
            return [];

        var bytes = reader.ReadBytes(count * 4);
        return [.. MemoryMarshal.Cast<byte, int>(bytes)];
    }

    private static T[] ReadStructArray<T>(BinaryReader reader, int count) where T : unmanaged
    {
        if (count <= 0)
            return [];

        var size = Unsafe.SizeOf<T>();
        var bytes = reader.ReadBytes(count * size);
        return [.. MemoryMarshal.Cast<byte, T>(bytes)];
    }
}
