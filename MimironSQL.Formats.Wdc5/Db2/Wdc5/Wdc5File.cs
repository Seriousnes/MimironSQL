using System.Buffers;
using System.Buffers.Binary;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;

using MimironSQL.Db2;
using MimironSQL.Formats;

using Security.Cryptography;

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

    public IEnumerable<RowHandle> EnumerateRowHandles()
    {
        for (var sectionIndex = 0; sectionIndex < ParsedSections.Count; sectionIndex++)
        {
            var section = ParsedSections[sectionIndex];
            for (var rowIndex = 0; rowIndex < section.NumRecords; rowIndex++)
            {
                var reader = CreateReaderAtRowStart(section, rowIndex);
                var id = GetVirtualId(section, rowIndex, reader);
                yield return new RowHandle
                {
                    SectionIndex = sectionIndex,
                    RowIndexInSection = rowIndex,
                    RowId = id
                };
            }
        }
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

    public bool TryGetRowHandle<TId>(TId id, out RowHandle handle) where TId : IEquatable<TId>, IComparable<TId>
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

        EnsureIndexesBuilt();

        var requestedId = key;

        if (_copyMap!.TryGetValue(key, out var sourceId))
            key = sourceId;

        if (!_idIndex!.TryGetValue(key, out var location))
        {
            handle = default;
            return false;
        }

        handle = new RowHandle
        {
            SectionIndex = location.SectionIndex,
            RowIndexInSection = location.RowIndexInSection,
            RowId = requestedId
        };
        return true;
    }

    public bool TryGetDenseStringTableIndex(Wdc5Row row, int fieldIndex, out int stringTableIndex)
        => row.TryGetDenseStringTableIndex(fieldIndex, out stringTableIndex);

    public bool TryGetRowById<TId>(TId id, out Wdc5Row row) where TId : IEquatable<TId>, IComparable<TId>
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

    public T ReadField<T>(RowHandle handle, int fieldIndex)
    {
        var type = typeof(T);
        if (type.IsEnum)
            return (T)ReadFieldBoxed(handle, Enum.GetUnderlyingType(type), fieldIndex);
        return (T)ReadFieldBoxed(handle, type, fieldIndex);
    }

    public void ReadFields(RowHandle handle, ReadOnlySpan<int> fieldIndices, Span<object> values)
    {
        if (fieldIndices.Length != values.Length)
            throw new ArgumentException("Field indices and values spans must have the same length.");

        for (var i = 0; i < fieldIndices.Length; i++)
            values[i] = ReadFieldBoxed(handle, typeof(object), fieldIndices[i]);
    }

    public void ReadAllFields(RowHandle handle, Span<object> values)
    {
        if (values.Length < Header.FieldsCount + 2)
            throw new ArgumentException($"Values span must have at least {Header.FieldsCount + 2} elements.");

        values[0] = ReadFieldBoxed(handle, typeof(int), Db2VirtualFieldIndex.Id);
        values[1] = ReadFieldBoxed(handle, typeof(int), Db2VirtualFieldIndex.ParentRelation);

        for (var i = 0; i < Header.FieldsCount; i++)
            values[i + 2] = ReadFieldBoxed(handle, typeof(object), i);
    }

    private object ReadFieldBoxed(RowHandle handle, Type type, int fieldIndex)
    {
        if ((uint)handle.SectionIndex >= (uint)ParsedSections.Count)
            throw new ArgumentException("Invalid section index in RowHandle.", nameof(handle));

        var section = ParsedSections[handle.SectionIndex];
        if ((uint)handle.RowIndexInSection >= (uint)section.NumRecords)
            throw new ArgumentException("Invalid row index in RowHandle.", nameof(handle));

        var reader = CreateReaderAtRowStart(section, handle.RowIndexInSection);
        var sourceId = GetVirtualId(section, handle.RowIndexInSection, reader);
        var id = handle.RowId;
        var globalRowIndex = section.FirstGlobalRecordIndex + handle.RowIndexInSection;

        var referenceKey = Header.Flags.HasFlag(Db2Flags.SecondaryKey) && section.IndexData is { Length: not 0 }
            ? section.IndexData[handle.RowIndexInSection]
            : handle.RowIndexInSection;

        _ = section.ParentLookupEntries.TryGetValue(referenceKey, out var parentRelationId);

        if (type.IsEnum)
            type = Enum.GetUnderlyingType(type);

        if (fieldIndex < 0)
        {
            if (fieldIndex == Db2VirtualFieldIndex.Id)
                return Convert.ChangeType(id, type);

            if (fieldIndex == Db2VirtualFieldIndex.ParentRelation)
                return Convert.ChangeType(parentRelationId, type);

            throw new NotSupportedException($"Unsupported virtual field index {fieldIndex}.");
        }

        if ((uint)fieldIndex >= (uint)Header.FieldsCount)
            throw new ArgumentOutOfRangeException(nameof(fieldIndex));

        if (type == typeof(string))
        {
            _ = TryGetString(section, handle.RowIndexInSection, globalRowIndex, reader, sourceId, fieldIndex, out var s);
            return s;
        }

        if (type == typeof(object))
        {
            ref readonly var fieldMeta = ref FieldMeta[fieldIndex];
            if (fieldMeta.Bits == 0)
            {
                _ = TryGetString(section, handle.RowIndexInSection, globalRowIndex, reader, sourceId, fieldIndex, out var s);
                return s;
            }

            type = typeof(int);
        }

        if (type.IsArray)
        {
            var elementType = type.GetElementType()!;
            if (elementType == typeof(double))
            {
                var floats = GetArray<float>(section, handle.RowIndexInSection, reader, sourceId, fieldIndex);
                var doubles = new double[floats.Length];
                for (var i = 0; i < floats.Length; i++)
                    doubles[i] = floats[i];
                return doubles;
            }
            return GetArrayBoxed(section, handle.RowIndexInSection, reader, sourceId, elementType, fieldIndex);
        }

        var fieldBitOffset = ColumnMeta[fieldIndex].RecordOffset;

        DecryptedRowLease decrypted = default;
        try
        {
            var isDecrypted = section.IsDecryptable;
            var localReader = isDecrypted
                ? (decrypted = DecryptRowBytes(section, handle.RowIndexInSection, reader, sourceId)).Reader
                : MoveToFieldStart(fieldIndex, reader);

            if (isDecrypted)
                localReader.PositionBits = fieldBitOffset;

            ref readonly var fieldMeta = ref FieldMeta[fieldIndex];
            ref readonly var columnMeta = ref ColumnMeta[fieldIndex];
            var palletData = PalletData[fieldIndex];
            var commonData = CommonData[fieldIndex];

            return ReadScalarBoxed(type, sourceId, ref localReader, fieldMeta, columnMeta, palletData, commonData);
        }
        finally
        {
            decrypted.Dispose();
        }
    }

    private static object ReadScalarBoxed(Type type, int id, ref BitReader reader, FieldMetaData fieldMeta, ColumnMetaData columnMeta, Value32[] palletData, Dictionary<int, Value32> commonData)
    {
        if (type == typeof(byte))
            return Wdc5FieldDecoder.ReadScalar<byte>(id, ref reader, fieldMeta, columnMeta, palletData, commonData);
        if (type == typeof(sbyte))
            return Wdc5FieldDecoder.ReadScalar<sbyte>(id, ref reader, fieldMeta, columnMeta, palletData, commonData);
        if (type == typeof(short))
            return Wdc5FieldDecoder.ReadScalar<short>(id, ref reader, fieldMeta, columnMeta, palletData, commonData);
        if (type == typeof(ushort))
            return Wdc5FieldDecoder.ReadScalar<ushort>(id, ref reader, fieldMeta, columnMeta, palletData, commonData);
        if (type == typeof(int))
            return Wdc5FieldDecoder.ReadScalar<int>(id, ref reader, fieldMeta, columnMeta, palletData, commonData);
        if (type == typeof(uint))
            return Wdc5FieldDecoder.ReadScalar<uint>(id, ref reader, fieldMeta, columnMeta, palletData, commonData);
        if (type == typeof(long))
            return Wdc5FieldDecoder.ReadScalar<long>(id, ref reader, fieldMeta, columnMeta, palletData, commonData);
        if (type == typeof(ulong))
            return Wdc5FieldDecoder.ReadScalar<ulong>(id, ref reader, fieldMeta, columnMeta, palletData, commonData);
        if (type == typeof(float))
            return Wdc5FieldDecoder.ReadScalar<float>(id, ref reader, fieldMeta, columnMeta, palletData, commonData);
        if (type == typeof(double))
            return (double)Wdc5FieldDecoder.ReadScalar<float>(id, ref reader, fieldMeta, columnMeta, palletData, commonData);

        throw new NotSupportedException($"Unsupported scalar type {type.FullName}.");
    }

    private object GetArrayBoxed(Wdc5Section section, int rowIndexInSection, BitReader reader, int id, Type elementType, int fieldIndex)
    {
        if (elementType == typeof(byte))
            return GetArray<byte>(section, rowIndexInSection, reader, id, fieldIndex);
        if (elementType == typeof(sbyte))
            return GetArray<sbyte>(section, rowIndexInSection, reader, id, fieldIndex);
        if (elementType == typeof(short))
            return GetArray<short>(section, rowIndexInSection, reader, id, fieldIndex);
        if (elementType == typeof(ushort))
            return GetArray<ushort>(section, rowIndexInSection, reader, id, fieldIndex);
        if (elementType == typeof(int))
            return GetArray<int>(section, rowIndexInSection, reader, id, fieldIndex);
        if (elementType == typeof(uint))
            return GetArray<uint>(section, rowIndexInSection, reader, id, fieldIndex);
        if (elementType == typeof(long))
            return GetArray<long>(section, rowIndexInSection, reader, id, fieldIndex);
        if (elementType == typeof(ulong))
            return GetArray<ulong>(section, rowIndexInSection, reader, id, fieldIndex);
        if (elementType == typeof(float))
            return GetArray<float>(section, rowIndexInSection, reader, id, fieldIndex);

        throw new NotSupportedException($"Unsupported array element type {elementType.FullName}.");
    }

    private T[] GetArray<T>(Wdc5Section section, int rowIndexInSection, BitReader reader, int id, int fieldIndex) where T : unmanaged
    {
        if ((uint)fieldIndex >= (uint)Header.FieldsCount)
            throw new ArgumentOutOfRangeException(nameof(fieldIndex));

        if (section.IsDecryptable)
        {
            using var decrypted = DecryptRowBytes(section, rowIndexInSection, reader, id);
            var localReader = decrypted.Reader;
            localReader.PositionBits = ColumnMeta[fieldIndex].RecordOffset;
            ref readonly var fieldMeta = ref FieldMeta[fieldIndex];
            ref readonly var columnMeta = ref ColumnMeta[fieldIndex];
            var palletData = PalletData[fieldIndex];

            return columnMeta.CompressionType switch
            {
                CompressionType.None => ReadNoneArray<T>(ref localReader, fieldMeta, columnMeta),
                CompressionType.PalletArray => ReadPalletArray<T>(ref localReader, columnMeta, palletData),
                _ => throw new NotSupportedException($"Array decode not supported for compression type {columnMeta.CompressionType} (field {fieldIndex})."),
            };
        }

        var localReader2 = MoveToFieldStart(fieldIndex, reader);
        ref readonly var fieldMeta2 = ref FieldMeta[fieldIndex];
        ref readonly var columnMeta2 = ref ColumnMeta[fieldIndex];
        var palletData2 = PalletData[fieldIndex];

        return columnMeta2.CompressionType switch
        {
            CompressionType.None => ReadNoneArray<T>(ref localReader2, fieldMeta2, columnMeta2),
            CompressionType.PalletArray => ReadPalletArray<T>(ref localReader2, columnMeta2, palletData2),
            _ => throw new NotSupportedException($"Array decode not supported for compression type {columnMeta2.CompressionType} (field {fieldIndex})."),
        };
    }

    private bool TryGetString(Wdc5Section section, int rowIndexInSection, int globalRowIndex, BitReader reader, int id, int fieldIndex, out string value)
    {
        if ((uint)fieldIndex >= (uint)Header.FieldsCount)
            throw new ArgumentOutOfRangeException(nameof(fieldIndex));
        return TryGetDenseString(section, globalRowIndex, reader, id, fieldIndex, out value) || TryGetInlineString(section, rowIndexInSection, reader, id, fieldIndex, out value);
    }

    private bool TryGetDenseString(Wdc5Section section, int globalRowIndex, BitReader reader, int id, int fieldIndex, out string value)
    {
        if (Header.Flags.HasFlag(Db2Flags.Sparse))
        {
            value = string.Empty;
            return false;
        }

        if ((uint)fieldIndex >= (uint)Header.FieldsCount)
            throw new ArgumentOutOfRangeException(nameof(fieldIndex));

        var fieldBitOffset = ColumnMeta[fieldIndex].RecordOffset;

        DecryptedRowLease decrypted = default;
        try
        {
            var isDecrypted = section.IsDecryptable;
            var localReader = isDecrypted
                ? (decrypted = DecryptRowBytes(section, -1, reader, id)).Reader
                : MoveToFieldStart(fieldIndex, reader);

            if (isDecrypted)
                localReader.PositionBits = fieldBitOffset;

            ref readonly var fieldMeta = ref FieldMeta[fieldIndex];
            ref readonly var columnMeta = ref ColumnMeta[fieldIndex];

            if (columnMeta is { CompressionType: CompressionType.PalletArray, Pallet.Cardinality: not 1 })
            {
                value = string.Empty;
                return false;
            }

            var offset = Wdc5FieldDecoder.ReadScalar<int>(id, ref localReader, fieldMeta, columnMeta, PalletData[fieldIndex], CommonData[fieldIndex]);
            if (offset == 0)
            {
                value = string.Empty;
                return true;
            }

            if (offset < 0)
            {
                value = string.Empty;
                return false;
            }

            var recordOffset = (long)(globalRowIndex * Header.RecordSize) - RecordsBlobSizeBytes;
            var fieldStartBytes = (long)(ColumnMeta[fieldIndex].RecordOffset >> 3);
            var stringIndex = recordOffset + fieldStartBytes + offset;

            if (stringIndex < 0)
                stringIndex = 0;

            if (stringIndex < section.StringTableBaseOffset)
            {
                value = string.Empty;
                return false;
            }

            var sectionEndExclusive = section.StringTableBaseOffset + section.Header.StringTableSize;
            if (stringIndex >= sectionEndExclusive || stringIndex is > int.MaxValue)
            {
                value = string.Empty;
                return false;
            }

            return TryReadNullTerminatedUtf8(DenseStringTableBytes.Span, startIndex: (int)stringIndex, endExclusive: sectionEndExclusive, out value);
        }
        finally
        {
            decrypted.Dispose();
        }
    }

    private bool TryGetInlineString(Wdc5Section section, int rowIndexInSection, BitReader reader, int id, int fieldIndex, out string value)
    {
        if (!Header.Flags.HasFlag(Db2Flags.Sparse))
        {
            value = string.Empty;
            return false;
        }

        if ((uint)fieldIndex >= (uint)Header.FieldsCount)
            throw new ArgumentOutOfRangeException(nameof(fieldIndex));

        var rowStartInSection = reader.OffsetBytes + (reader.PositionBits >> 3);
        var rowSizeBytes = (int)section.SparseEntries[rowIndexInSection].Size;

        ReadOnlySpan<byte> recordBytes = section.RecordsData;
        var rowEndExclusive = rowStartInSection + rowSizeBytes;

        DecryptedRowLease decrypted = default;
        try
        {
            var localReader = reader;
            localReader.PositionBits = reader.PositionBits;

            if (section.IsDecryptable)
            {
                decrypted = DecryptRowBytes(section, rowIndexInSection, reader, id);
                recordBytes = decrypted.Bytes.AsSpan(0, rowSizeBytes);
                rowEndExclusive = rowSizeBytes;
                rowStartInSection = 0;

                localReader = decrypted.Reader;
                localReader.PositionBits = 0;
            }

            for (var i = 0; i < fieldIndex; i++)
                SkipSparseField(ref localReader, i, recordBytes, rowEndExclusive, id);

            var fieldStart = localReader.OffsetBytes + (localReader.PositionBits >> 3);
            if (fieldStart >= 0 && fieldStart < rowEndExclusive && TryReadNullTerminatedUtf8(recordBytes, startIndex: fieldStart, endExclusive: rowEndExclusive, out value))
                return true;

            value = string.Empty;
            return false;
        }
        finally
        {
            decrypted.Dispose();
        }
    }

    private void SkipSparseField(ref BitReader reader, int fieldIndex, ReadOnlySpan<byte> recordBytes, int rowEndExclusive, int id)
    {
        ref readonly var fieldMeta = ref FieldMeta[fieldIndex];
        ref readonly var columnMeta = ref ColumnMeta[fieldIndex];

        if (columnMeta is { CompressionType: CompressionType.None })
        {
            var bitSize = 32 - fieldMeta.Bits;
            if (bitSize <= 0)
                bitSize = columnMeta.Immediate.BitWidth;

            if (bitSize == 32)
            {
                var currentBytePos = reader.OffsetBytes + (reader.PositionBits >> 3);
                var terminatorIndex = recordBytes[currentBytePos..rowEndExclusive].IndexOf((byte)0);
                if (terminatorIndex >= 0)
                {
                    reader.PositionBits += (terminatorIndex + 1) * 8;
                    return;
                }
            }

            reader.PositionBits += bitSize;
        }
        else
        {
            _ = Wdc5FieldDecoder.ReadScalar<long>(id, ref reader, fieldMeta, columnMeta, PalletData[fieldIndex], CommonData[fieldIndex]);
        }
    }

    private static bool TryReadNullTerminatedUtf8(ReadOnlySpan<byte> bytes, int startIndex, int endExclusive, out string value)
    {
        if (startIndex < 0 || startIndex >= bytes.Length || endExclusive < 0 || endExclusive > bytes.Length || startIndex >= endExclusive)
        {
            value = string.Empty;
            return false;
        }

        var span = bytes[startIndex..endExclusive];
        var terminatorIndex = span.IndexOf((byte)0);
        if (terminatorIndex < 0)
        {
            value = string.Empty;
            return false;
        }

        if (terminatorIndex == 0)
        {
            value = string.Empty;
            return true;
        }

        try
        {
            var utf8Strict = new UTF8Encoding(encoderShouldEmitUTF8Identifier: false, throwOnInvalidBytes: true);
            value = utf8Strict.GetString(span[..terminatorIndex]);
            return true;
        }
        catch (DecoderFallbackException)
        {
            value = string.Empty;
            return false;
        }
    }

    private BitReader MoveToFieldStart(int fieldIndex, BitReader reader)
    {
        var localReader = reader;
        var fieldBitOffset = ColumnMeta[fieldIndex].RecordOffset;

        if (Header.Flags.HasFlag(Db2Flags.Sparse))
            localReader.PositionBits = reader.PositionBits + fieldBitOffset;
        else
            localReader.PositionBits = fieldBitOffset;

        return localReader;
    }

    private static T[] ReadNoneArray<T>(ref BitReader reader, FieldMetaData fieldMeta, ColumnMetaData columnMeta) where T : unmanaged
    {
        var bitSize = 32 - fieldMeta.Bits;
        if (bitSize <= 0)
            bitSize = columnMeta.Immediate.BitWidth;

        var elementCount = columnMeta.Size / (Unsafe.SizeOf<T>() * 8);
        var array = new T[elementCount];
        for (var i = 0; i < array.Length; i++)
        {
            var raw = reader.ReadUInt64(bitSize);
            array[i] = Unsafe.As<ulong, T>(ref raw);
        }
        return array;
    }

    private static T[] ReadPalletArray<T>(ref BitReader reader, ColumnMetaData columnMeta, Value32[] palletData) where T : unmanaged
    {
        var cardinality = columnMeta.Pallet.Cardinality;
        var palletArrayIndex = reader.ReadUInt32(columnMeta.Pallet.BitWidth);

        var array = new T[cardinality];
        for (var i = 0; i < array.Length; i++)
            array[i] = palletData[i + cardinality * (int)palletArrayIndex].GetValue<T>();

        return array;
    }

    private readonly struct DecryptedRowLease : IDisposable
    {
        private readonly byte[]? _buffer;
        private readonly int _clearLength;

        public DecryptedRowLease(byte[] buffer, int clearLength)
        {
            _buffer = buffer;
            _clearLength = clearLength;
            Reader = new BitReader(buffer) { OffsetBytes = 0, PositionBits = 0 };
        }

        public BitReader Reader { get; }

        public byte[] Bytes => _buffer ?? [];

        public void Dispose()
        {
            if (_buffer is null)
                return;

            if (_clearLength > 0 && _clearLength <= _buffer.Length)
                Array.Clear(_buffer, 0, _clearLength);

            ArrayPool<byte>.Shared.Return(_buffer);
        }
    }

    private DecryptedRowLease DecryptRowBytes(Wdc5Section section, int rowIndexInSection, BitReader reader, int id)
    {
        if (!section.IsDecryptable)
            throw new InvalidOperationException("Row is not decryptable.");

        var rowStartInSection = reader.OffsetBytes + (reader.PositionBits >> 3);
        var rowSizeBytes = Header.Flags.HasFlag(Db2Flags.Sparse)
            ? (int)section.SparseEntries[rowIndexInSection].Size
            : Header.RecordSize;

        if (rowSizeBytes < 0)
            throw new InvalidDataException("Row size is negative.");

        var rowEndExclusive = (long)rowStartInSection + rowSizeBytes;
        if (rowStartInSection < 0 || rowEndExclusive < 0 || rowEndExclusive > section.RecordsData.Length)
            throw new InvalidDataException("Encrypted row points outside section record data.");

        var buffer = ArrayPool<byte>.Shared.Rent(rowSizeBytes + 8);
        var dst = buffer.AsSpan(0, rowSizeBytes);
        section.RecordsData.AsSpan(rowStartInSection, rowSizeBytes).CopyTo(dst);
        Array.Clear(buffer, rowSizeBytes, 8);

        var nonceId = Options is { EncryptedRowNonceStrategy: Wdc5EncryptedRowNonceStrategy.SourceId } ? id : id;
        Span<byte> nonce = stackalloc byte[8];
        BinaryPrimitives.WriteUInt64LittleEndian(nonce, unchecked((ulong)nonceId));

        using (var salsa = new Salsa20(section.TactKey.Span, nonce))
        {
            var span = buffer.AsSpan(0, rowSizeBytes);
            salsa.Transform(span, span);
        }

        return new DecryptedRowLease(buffer, clearLength: rowSizeBytes);
    }
}
