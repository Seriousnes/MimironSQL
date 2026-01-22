using MimironSQL.Db2;
using System;
using System.Collections.Generic;
using System.IO;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;

namespace MimironSQL.Db2.Wdc5;

public sealed class Wdc5File
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

    internal byte[] DenseStringTableBytes { get; } = Array.Empty<byte>();
    internal int RecordsBlobSizeBytes { get; }

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

        if (reader.BaseStream.Length < HeaderSize)
            throw new InvalidDataException("DB2 file is too small to be valid.");

        var magic = reader.ReadUInt32();
        if (magic != Wdc5Magic)
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
                palletData[i] = Array.Empty<Value32>();
            }
        }

        var commonData = new Dictionary<int, Value32>[columnMeta.Length];
        for (var i = 0; i < columnMeta.Length; i++)
        {
            if (columnMeta[i].CompressionType == CompressionType.Common)
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
                if (section.TactKeyLookup != 0 && Options.TactKeyProvider is not null)
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
                    stringTableBytes = Array.Empty<byte>();

                    if (reader.BaseStream.Position != section.OffsetRecordsEndOffset)
                        throw new InvalidDataException("WDC5 sparse section parsing desynced: expected OffsetRecordsEndOffset.");
                }

                var isEncrypted = section.TactKeyLookup != 0;
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
                if (indexData.Length > 0 && indexData.All(x => x == 0))
                    indexData = [.. Enumerable.Range(minIndex + previousRecordCount, section.NumRecords)];

                // copy table
                var copyData = new Dictionary<int, int>();
                if (section.CopyTableCount > 0)
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
                SparseEntry[] sparseEntries = Array.Empty<SparseEntry>();
                if (section.OffsetMapIDCount > 0)
                    sparseEntries = ReadStructArray<SparseEntry>(reader, section.OffsetMapIDCount);

                // secondary key sparse index data (not fully surfaced yet)
                if (section.OffsetMapIDCount > 0 && flags.HasFlag(Db2Flags.SecondaryKey))
                {
                    var sparseIndexData = ReadInt32Array(reader, section.OffsetMapIDCount);
                    if (section.IndexDataSize > 0 && indexData.Length != sparseIndexData.Length)
                        throw new InvalidDataException("WDC5 sparse index data length mismatch.");
                    indexData = sparseIndexData;
                }

                // parent lookup data (parsed only enough to advance stream)
                if (section.ParentLookupDataSize > 0)
                {
                    var numRecords = reader.ReadInt32();
                    _ = reader.ReadInt32(); // minId
                    _ = reader.ReadInt32(); // maxId
                    _ = reader.ReadBytes(numRecords * 8);
                }

                // if OffsetMap exists but we didn't read sparse index earlier, WDC5 can have it here too
                if (section.OffsetMapIDCount > 0 && !flags.HasFlag(Db2Flags.SecondaryKey))
                {
                    var sparseIndexData = ReadInt32Array(reader, section.OffsetMapIDCount);
                    if (section.IndexDataSize > 0 && indexData.Length != sparseIndexData.Length)
                        throw new InvalidDataException("WDC5 sparse index data length mismatch.");
                    indexData = sparseIndexData;
                }

                var sparseStarts = flags.HasFlag(Db2Flags.Sparse)
                    ? Wdc5Section.BuildSparseRecordStartBits(sparseEntries, section.FileOffset, recordDataSizeBytes)
                    : Array.Empty<int>();

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
        DenseStringTableBytes = [.. denseStringTableBytes];
        RecordsBlobSizeBytes = recordsBlobSizeBytes;

        if (recordsCount > 0 && parsedSections.Count == 0)
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
                yield return new Wdc5Row(this, section, reader, globalIndex, rowIndex, id, sourceId: id);
                globalIndex++;
            }
        }
    }

    public bool TryGetRowById(int id, out Wdc5Row row)
    {
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
        row = new Wdc5Row(this, section, reader, globalRowIndex: location.GlobalRecordIndex, rowIndexInSection: location.RowIndexInSection, id: requestedId, sourceId: id);
        return true;
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

        if (section.SparseRecordStartBits.Length == 0)
            throw new InvalidDataException("Sparse WDC5 section missing SparseRecordStartBits.");

        var startBits = section.SparseRecordStartBits[rowIndex];
        var startBytes = startBits >> 3;
        var sizeBytes = (int)section.SparseEntries[rowIndex].Size;
        if ((uint)startBytes > (uint)section.RecordsData.Length || (uint)(startBytes + sizeBytes) > (uint)section.RecordsData.Length)
            throw new InvalidDataException("Sparse WDC5 row points outside section record data.");

        reader.PositionBits = startBits;
        return reader;
    }

    private int GetVirtualId(Wdc5Section section, int rowIndex, BitReader readerAtStart)
    {
        // A) Prefer IndexData when present.
        if (section.IndexData.Length != 0)
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

            if (columnMeta.CompressionType == CompressionType.Common)
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
            return Array.Empty<int>();

        var bytes = reader.ReadBytes(count * 4);
        return MemoryMarshal.Cast<byte, int>(bytes).ToArray();
    }

    private static T[] ReadStructArray<T>(BinaryReader reader, int count) where T : unmanaged
    {
        if (count <= 0)
            return Array.Empty<T>();

        var size = Unsafe.SizeOf<T>();
        var bytes = reader.ReadBytes(count * size);
        return MemoryMarshal.Cast<byte, T>(bytes).ToArray();
    }
}
