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

    public required Wdc5Header Header { get; init; }
    public required IReadOnlyList<Wdc5SectionHeader> Sections { get; init; }
    public required IReadOnlyList<Wdc5Section> ParsedSections { get; init; }
    public required FieldMetaData[] FieldMeta { get; init; }
    public required ColumnMetaData[] ColumnMeta { get; init; }
    public required Value32[][] PalletData { get; init; }
    public required Dictionary<int, Value32>[] CommonData { get; init; }

    public int TotalSectionRecordCount => ParsedSections.Sum(s => s.NumRecords);

    public IEnumerable<Wdc5Row> EnumerateRows()
    {
        var globalIndex = 0;
        foreach (var section in ParsedSections)
        {
            for (var i = 0; i < section.NumRecords; i++)
            {
                var reader = new BitReader(section.RecordsData)
                {
                    OffsetBytes = 0,
                    PositionBits = 0,
                };

                if (!Header.Flags.HasFlag(Db2Flags.Sparse))
                {
                    reader.OffsetBytes = i * Header.RecordSize;
                }
                else
                {
                    if (section.SparseRecordStartBits.Length == 0)
                        throw new InvalidDataException("Sparse WDC5 section missing SparseRecordStartBits.");
                    reader.PositionBits = section.SparseRecordStartBits[i];
                }

                var id = section.GetRowIdOrDefault(i, defaultId: -1);
                yield return new Wdc5Row(this, section, reader, globalIndex, i, id);
                globalIndex++;
            }
        }
    }

    public static Wdc5File Open(string path)
    {
        using var stream = File.OpenRead(path);
        using var reader = new BinaryReader(stream, Encoding.UTF8, leaveOpen: false);

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

        if (sectionsCount != 0 && recordsCount != 0)
        {
            var previousRecordCount = 0;
            foreach (var section in sections)
            {
                reader.BaseStream.Position = section.FileOffset;

                byte[] recordsData;
                byte[] stringTableBytes;
                if (!flags.HasFlag(Db2Flags.Sparse))
                {
                    recordsData = reader.ReadBytes(section.NumRecords * recordSize);
                    Array.Resize(ref recordsData, recordsData.Length + 8);
                    stringTableBytes = reader.ReadBytes(section.StringTableSize);
                }
                else
                {
                    recordsData = reader.ReadBytes(section.OffsetRecordsEndOffset - section.FileOffset);
                    stringTableBytes = Array.Empty<byte>();

                    if (reader.BaseStream.Position != section.OffsetRecordsEndOffset)
                        throw new InvalidDataException("WDC5 sparse section parsing desynced: expected OffsetRecordsEndOffset.");
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

                var sparseStarts = Wdc5Section.BuildSparseRecordStartBits(sparseEntries);

                parsedSections.Add(new Wdc5Section
                {
                    Header = section,
                    RecordsData = recordsData,
                    StringTableBytes = stringTableBytes,
                    IndexData = indexData,
                    CopyData = copyData,
                    SparseEntries = sparseEntries,
                    SparseRecordStartBits = sparseStarts,
                });

                previousRecordCount += section.NumRecords;
            }
        }

        return new Wdc5File
        {
            Header = header,
            Sections = sections,
            ParsedSections = parsedSections,
            FieldMeta = fieldMeta,
            ColumnMeta = columnMeta,
            PalletData = palletData,
            CommonData = commonData,
        };
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
