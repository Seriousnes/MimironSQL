using System.Buffers;
using System.Buffers.Binary;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;

using MimironSQL.Db2;

using Security.Cryptography;

namespace MimironSQL.Formats.Wdc5.Db2;

/// <summary>
/// Represents an opened WDC5 DB2 file.
/// </summary>
public sealed class Wdc5File : IDb2File<RowHandle>, IDb2DenseStringTableIndexProvider<RowHandle>
{
    private const int HeaderSize = 200;
    private const uint Wdc5Magic = 0x35434457; // "WDC5"

    /// <summary>
    /// Gets the parsed WDC5 header.
    /// </summary>
    public Wdc5Header Header { get; }

    /// <summary>
    /// Gets the raw section headers.
    /// </summary>
    public IReadOnlyList<Wdc5SectionHeader> Sections { get; }

    /// <summary>
    /// Gets the parsed sections.
    /// </summary>
    public IReadOnlyList<Wdc5Section> ParsedSections { get; }

    /// <summary>
    /// Gets per-field metadata.
    /// </summary>
    public FieldMetaData[] FieldMeta { get; }

    /// <summary>
    /// Gets per-column metadata.
    /// </summary>
    public ColumnMetaData[] ColumnMeta { get; }

    /// <summary>
    /// Gets pallet data blocks per field.
    /// </summary>
    public uint[][] PalletData { get; }

    /// <summary>
    /// Gets common-value dictionaries per field.
    /// </summary>
    public Dictionary<int, uint>[] CommonData { get; }

    private ReadOnlyMemory<byte> _denseStringTableBytes = ReadOnlyMemory<byte>.Empty;

    /// <inheritdoc />
    public ReadOnlyMemory<byte> DenseStringTableBytes
    {
        get
        {
            EnsureDenseStringTableMaterialized();
            return _denseStringTableBytes;
        }
    }

    /// <summary>
    /// Gets the total size in bytes of the concatenated record data blob.
    /// </summary>
    public int RecordsBlobSizeBytes { get; }

    /// <inheritdoc />
    public Type RowType => typeof(RowHandle);

    IDb2FileHeader IDb2File.Header => Header;

    /// <inheritdoc />
    public Db2Flags Flags => Header.Flags;

    /// <inheritdoc />
    public int RecordsCount => Header.RecordsCount;

    /// <summary>
    /// Gets the total number of records across all parsed sections.
    /// </summary>
    public int TotalSectionRecordCount => ParsedSections.Sum(s => s.NumRecords);

    private Dictionary<int, (int SectionIndex, int RowIndexInSection, int GlobalRecordIndex)>? _idIndex;
    private int _pkLookupCount;
    private const int BuildIdIndexAfterPkLookups = 2;

    private readonly Stream _stream;
    private readonly BinaryReader _reader;

    private byte[]? _cachedRowBytes;
    private int _cachedRowSizeBytes;
    private int _cachedRowStartBitOffset;
    private int _cachedSectionIndex;
    private int _cachedRowIndexInSection;
    private bool _cachedRowValid;

    internal Wdc5FileOptions Options { get; }

    /// <summary>
    /// Opens and parses a WDC5 file from a stream.
    /// </summary>
    /// <param name="stream">A seekable stream containing WDC5 data.</param>
    public Wdc5File(Stream stream) : this(stream, options: null)
    {
    }

    /// <summary>
    /// Opens and parses a WDC5 file from a stream.
    /// </summary>
    /// <param name="stream">A seekable stream containing WDC5 data.</param>
    /// <param name="options">Optional parsing and decryption options.</param>
    public Wdc5File(Stream stream, Wdc5FileOptions? options)
    {
        ArgumentNullException.ThrowIfNull(stream);
        if (!stream.CanSeek)
            throw new NotSupportedException("WDC5 parsing requires a seekable Stream.");

        Options = options ?? new Wdc5FileOptions();

        _stream = stream;
        _reader = new BinaryReader(_stream, Encoding.UTF8, leaveOpen: true);

        _cachedSectionIndex = -1;
        _cachedRowIndexInSection = -1;

        _stream.Position = 0;

        if (_reader.BaseStream.Length is < HeaderSize)
            throw new InvalidDataException("DB2 file is too small to be valid.");

        var magic = _reader.ReadUInt32();
        if (magic is not Wdc5Magic)
        {
            Span<byte> headerBytes = stackalloc byte[4];
            Unsafe.WriteUnaligned(ref headerBytes[0], magic);
            var detected = Db2FormatDetector.Detect(headerBytes);
            throw new InvalidDataException($"Expected WDC5 but found {detected}.");
        }

        var schemaVersion = _reader.ReadUInt32();
        var schemaString = Encoding.UTF8.GetString(_reader.ReadBytes(128)).TrimEnd('\0');
        var recordsCount = _reader.ReadInt32();
        var fieldsCount = _reader.ReadInt32();
        var recordSize = _reader.ReadInt32();
        var stringTableSize = _reader.ReadInt32();
        var tableHash = _reader.ReadUInt32();
        var layoutHash = _reader.ReadUInt32();
        var minIndex = _reader.ReadInt32();
        var maxIndex = _reader.ReadInt32();
        var locale = _reader.ReadInt32();
        var flags = (Db2Flags)_reader.ReadUInt16();
        var idFieldIndex = _reader.ReadUInt16();
        var totalFieldsCount = _reader.ReadInt32();
        var packedDataOffset = _reader.ReadInt32();
        var lookupColumnCount = _reader.ReadInt32();
        var columnMetaDataSize = _reader.ReadInt32();
        var commonDataSize = _reader.ReadInt32();
        var palletDataSize = _reader.ReadInt32();
        var sectionsCount = _reader.ReadInt32();

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
            var tactKeyLookup = _reader.ReadUInt64();
            var fileOffset = _reader.ReadInt32();
            var numRecords = _reader.ReadInt32();
            var sectionStringTableSize = _reader.ReadInt32();
            var offsetRecordsEndOffset = _reader.ReadInt32();
            var indexDataSize = _reader.ReadInt32();
            var parentLookupDataSize = _reader.ReadInt32();
            var offsetMapIdCount = _reader.ReadInt32();
            var copyTableCount = _reader.ReadInt32();
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
            fieldMeta[i] = new FieldMetaData(_reader.ReadInt16(), _reader.ReadInt16());

        var columnMeta = new ColumnMetaData[fieldsCount];
        var metaBytes = _reader.ReadBytes(Unsafe.SizeOf<ColumnMetaData>() * fieldsCount);
        MemoryMarshal.Cast<byte, ColumnMetaData>(metaBytes).CopyTo(columnMeta);

        var palletData = new uint[columnMeta.Length][];
        for (var i = 0; i < columnMeta.Length; i++)
        {
            switch (columnMeta[i].CompressionType)
            {
                case CompressionType.Pallet or CompressionType.PalletArray:
                    {
                        var count = checked((int)columnMeta[i].AdditionalDataSize / 4);
                        var bytes = _reader.ReadBytes(count * 4);
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
                            var id = _reader.ReadInt32();
                            var value = _reader.ReadUInt32();
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

        var parsedSections = new List<Wdc5Section>(sections.Count);
        var shouldMaterialize = Options.RecordLoadingMode == Wdc5RecordLoadingMode.Eager;
        var denseStringTableBytes = shouldMaterialize && stringTableSize > 0 ? new byte[stringTableSize] : [];
        var denseStringWriteOffset = 0;
        var recordsBlobSizeBytes = 0;

        if (sectionsCount != 0 && recordsCount != 0)
        {
            var previousRecordCount = 0;
            var previousStringTableSize = 0;
            var previousRecordBlobSizeBytes = 0;
            foreach (var section in sections)
            {
                _reader.BaseStream.Position = section.FileOffset;

                ReadOnlyMemory<byte> tactKey = ReadOnlyMemory<byte>.Empty;
                if (section is { TactKeyLookup: not 0 } && Options.TactKeyProvider is not null)
                    Options.TactKeyProvider.TryGetKey(section.TactKeyLookup, out tactKey);

                byte[]? recordsData = null;
                int recordDataSizeBytes;
                if (!flags.HasFlag(Db2Flags.Sparse))
                {
                    recordDataSizeBytes = section.NumRecords * recordSize;

                    if (shouldMaterialize)
                    {
                        recordsData = new byte[checked(recordDataSizeBytes + 8)];
                        ReadExactly(_reader, recordsData, recordDataSizeBytes);

                        if (section.StringTableSize > 0)
                        {
                            if (denseStringWriteOffset + section.StringTableSize > denseStringTableBytes.Length)
                                throw new InvalidDataException("WDC5 dense string table overflow: section string table exceeds declared size.");

                            ReadExactly(_reader, denseStringTableBytes, start: denseStringWriteOffset, count: section.StringTableSize);
                            denseStringWriteOffset += section.StringTableSize;
                        }
                    }
                    else
                    {
                        _reader.BaseStream.Position += recordDataSizeBytes;
                        if (section.StringTableSize > 0)
                            _reader.BaseStream.Position += section.StringTableSize;
                    }
                }
                else
                {
                    recordDataSizeBytes = section.OffsetRecordsEndOffset - section.FileOffset;

                    if (shouldMaterialize)
                    {
                        recordsData = new byte[checked(recordDataSizeBytes + 8)];
                        ReadExactly(_reader, recordsData, recordDataSizeBytes);

                        if (_reader.BaseStream.Position != section.OffsetRecordsEndOffset)
                            throw new InvalidDataException("WDC5 sparse section parsing desynced: expected OffsetRecordsEndOffset.");
                    }
                    else
                    {
                        _reader.BaseStream.Position += recordDataSizeBytes;
                        if (_reader.BaseStream.Position != section.OffsetRecordsEndOffset)
                            throw new InvalidDataException("WDC5 sparse section parsing desynced: expected OffsetRecordsEndOffset.");
                    }
                }

                var isEncrypted = section is { TactKeyLookup: not 0 };
                var shouldSkipEncryptedSection = false;
                if (isEncrypted)
                {
                    if (tactKey.IsEmpty)
                    {
                        shouldSkipEncryptedSection = true;
                    }
                }

                // index data
                var indexData = ReadInt32Array(_reader, section.IndexDataSize / 4);
                if (indexData is { Length: > 0 } && indexData.All(x => x == 0))
                    indexData = [.. Enumerable.Range(minIndex + previousRecordCount, section.NumRecords)];

                // copy table
                var copyData = new Dictionary<int, int>();
                if (section is { CopyTableCount: > 0 })
                {
                    for (var i = 0; i < section.CopyTableCount; i++)
                    {
                        var destinationRowId = _reader.ReadInt32();
                        var sourceRowId = _reader.ReadInt32();
                        if (destinationRowId != sourceRowId)
                            copyData[destinationRowId] = sourceRowId;
                    }
                }

                // offset map / sparse entries
                SparseEntry[] sparseEntries = [];
                if (section is { OffsetMapIDCount: > 0 })
                    sparseEntries = ReadStructArray<SparseEntry>(_reader, section.OffsetMapIDCount);

                // secondary key sparse index data (not fully surfaced yet)
                if (section is { OffsetMapIDCount: > 0 } && flags.HasFlag(Db2Flags.SecondaryKey))
                {
                    var sparseIndexData = ReadInt32Array(_reader, section.OffsetMapIDCount);
                    if (section is { IndexDataSize: > 0 } && indexData.Length != sparseIndexData.Length)
                        throw new InvalidDataException("WDC5 sparse index data length mismatch.");
                    indexData = sparseIndexData;
                }

                // parent lookup data (parsed only enough to advance stream)
                var parentLookupEntries = new Dictionary<int, int>();
                if (section is { ParentLookupDataSize: > 0 })
                {
                    var numRecords = _reader.ReadInt32();
                    _reader.ReadInt32(); // minId
                    _reader.ReadInt32(); // maxId

                    for (var i = 0; i < numRecords; i++)
                    {
                        var index = _reader.ReadInt32();
                        var id = _reader.ReadInt32();
                        parentLookupEntries[index] = id;
                    }
                }

                // if OffsetMap exists but we didn't read sparse index earlier, WDC5 can have it here too
                if (section is { OffsetMapIDCount: > 0 } && !flags.HasFlag(Db2Flags.SecondaryKey))
                {
                    var sparseIndexData = ReadInt32Array(_reader, section.OffsetMapIDCount);
                    if (section is { IndexDataSize: > 0 } && indexData.Length != sparseIndexData.Length)
                        throw new InvalidDataException("WDC5 sparse index data length mismatch.");
                    indexData = sparseIndexData;
                }

                var sparseStarts = flags.HasFlag(Db2Flags.Sparse)
                    ? Wdc5Section.BuildSparseRecordStartBits(sparseEntries, section.FileOffset, recordDataSizeBytes)
                    : [];

                if (!shouldSkipEncryptedSection)
                {
                    var parsedSection = new Wdc5Section
                    {
                        Header = section,
                        FirstGlobalRecordIndex = previousRecordCount,
                        RecordsData = recordsData,
                        RecordDataSizeBytes = recordDataSizeBytes,
                        RecordsBaseOffsetInBlob = previousRecordBlobSizeBytes,
                        StringTableBaseOffset = previousStringTableSize,
                        StringTableBytes = [],
                        IndexData = indexData,
                        CopyData = copyData,
                        ParentLookupEntries = parentLookupEntries,
                        SparseEntries = sparseEntries,
                        SparseRecordStartBits = sparseStarts,
                        TactKey = tactKey,
                    };

                    if (flags.HasFlag(Db2Flags.Sparse))
                    {
                        parsedSection.SparseOffsetTable = new Lazy<Wdc5SparseOffsetTable>(() => BuildSparseOffsetTable(parsedSection), isThreadSafe: true);
                        if (Options.EagerSparseOffsetTable)
                            _ = parsedSection.SparseOffsetTable.Value;
                    }

                    parsedSections.Add(parsedSection);
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
        _denseStringTableBytes = new ReadOnlyMemory<byte>(denseStringTableBytes);
        RecordsBlobSizeBytes = recordsBlobSizeBytes;

        if (recordsCount > 0 && parsedSections is { Count: 0 })
            throw new NotSupportedException("All WDC5 sections are encrypted or unreadable (missing TACT keys or placeholder section data).");
    }

    internal Wdc5FieldAccessor CreateFieldAccessor(int sectionIndex, int fieldIndex)
    {
        if ((uint)sectionIndex >= (uint)ParsedSections.Count)
            throw new ArgumentOutOfRangeException(nameof(sectionIndex));

        if (Header.Flags.HasFlag(Db2Flags.Sparse))
            throw new NotSupportedException("Use CreateSparseFieldAccessor for sparse WDC5 files.");

        var section = ParsedSections[sectionIndex];
        EnsureSectionRecordsMaterialized(section);
        return new Wdc5FieldAccessor(this, section, fieldIndex);
    }

    internal Wdc5SparseFieldAccessor CreateSparseFieldAccessor(int sectionIndex, int fieldIndex)
    {
        if ((uint)sectionIndex >= (uint)ParsedSections.Count)
            throw new ArgumentOutOfRangeException(nameof(sectionIndex));

        if (!Header.Flags.HasFlag(Db2Flags.Sparse))
            throw new NotSupportedException("Sparse field accessors require sparse WDC5 files.");

        var section = ParsedSections[sectionIndex];
        EnsureSectionRecordsMaterialized(section);
        return new Wdc5SparseFieldAccessor(this, section, fieldIndex, GetOrCreateSparseOffsetTable(section));
    }

    internal int GetSourceIdForAccessor(Wdc5Section section, int rowIndex)
    {
        if ((uint)rowIndex >= (uint)section.NumRecords)
            throw new ArgumentOutOfRangeException(nameof(rowIndex));

        if (section.IndexData is { Length: not 0 })
            return section.IndexData[rowIndex];

        EnsureSectionRecordsMaterialized(section);
        var reader = CreateReaderAtRowStart(section, rowIndex, out _, out _, out _);
        return GetVirtualId(section, rowIndex, reader);
    }

    internal void EnsureSectionRecordsMaterialized(Wdc5Section section)
    {
        if (section.RecordsData is not null)
            return;

        var recordsData = new byte[checked(section.RecordDataSizeBytes + 8)];
        _reader.BaseStream.Position = section.Header.FileOffset;
        ReadExactly(_reader, recordsData, section.RecordDataSizeBytes);
        section.RecordsData = recordsData;
    }

    private Wdc5SparseOffsetTable GetOrCreateSparseOffsetTable(Wdc5Section section)
        => section.SparseOffsetTable?.Value ?? throw new InvalidOperationException("Sparse offset tables are only available for sparse WDC5 sections.");

    private Wdc5SparseOffsetTable BuildSparseOffsetTable(Wdc5Section section)
    {
        EnsureSectionRecordsMaterialized(section);

        var positions = new int[checked(section.NumRecords * Header.FieldsCount)];
        for (var rowIndex = 0; rowIndex < section.NumRecords; rowIndex++)
        {
            var readerAtStart = CreateReaderAtRowStart(section, rowIndex, out var recordBytes, out var rowStartByte, out var rowSizeBytes);
            var rowEndExclusive = rowStartByte + rowSizeBytes;
            var id = GetSourceIdForAccessor(section, rowIndex);
            var localReader = readerAtStart;

            for (var fieldIndex = 0; fieldIndex < Header.FieldsCount; fieldIndex++)
            {
                positions[(rowIndex * Header.FieldsCount) + fieldIndex] = localReader.PositionBits;
                SkipSparseField(ref localReader, fieldIndex, recordBytes, rowEndExclusive, id);
            }
        }

        return new Wdc5SparseOffsetTable(Header.FieldsCount, section.NumRecords, positions);
    }

    private static void ReadExactly(BinaryReader reader, byte[] buffer, int count)
        => ReadExactly(reader, buffer, start: 0, count);

    private static void ReadExactly(BinaryReader reader, byte[] buffer, int start, int count)
    {
        var totalRead = 0;
        while (totalRead < count)
        {
            var read = reader.Read(buffer, start + totalRead, count - totalRead);
            if (read <= 0)
                throw new EndOfStreamException("Unexpected end of stream while reading WDC5 section data.");
            totalRead += read;
        }
    }

    /// <inheritdoc />
    public IEnumerable<RowHandle> EnumerateRowHandles()
    {
        for (var sectionIndex = 0; sectionIndex < ParsedSections.Count; sectionIndex++)
        {
            var section = ParsedSections[sectionIndex];
            for (var rowIndex = 0; rowIndex < section.NumRecords; rowIndex++)
            {
                // Prefer IndexData when present; avoids any record reads/materialization.
                int id;
                if (section.IndexData is { Length: not 0 })
                {
                    id = section.IndexData[rowIndex];
                }
                else if (section.RecordsData is not null)
                {
                    var reader = CreateReaderAtRowStart(section, rowIndex, out _, out _, out _);
                    id = GetVirtualId(section, rowIndex, reader);
                }
                else
                {
                    var rowBytes = GetRowBytesFromStream(sectionIndex, section, rowIndex, out _, out var rowStartBitOffset);
                    var reader = new Wdc5RowReader(rowBytes, positionBits: rowStartBitOffset);
                    id = GetVirtualId(section, rowIndex, reader);
                }

                yield return new RowHandle(sectionIndex, rowIndex, id);
            }
        }
    }


    /// <inheritdoc />
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

        var requestedId = key;

        // Preserve "copy table wins" behavior without materializing a global copy map.
        if (TryResolveCopySourceId(key, out var sourceId))
            key = sourceId;

        if (_idIndex is not null)
        {
            if (!_idIndex.TryGetValue(key, out var location))
            {
                handle = default;
                return false;
            }

            handle = new RowHandle(location.SectionIndex, location.RowIndexInSection, requestedId);
            return true;
        }

        _pkLookupCount++;
        if (_pkLookupCount >= BuildIdIndexAfterPkLookups)
        {
            EnsureIdIndexBuilt();

            if (!_idIndex!.TryGetValue(key, out var location))
            {
                handle = default;
                return false;
            }

            handle = new RowHandle(location.SectionIndex, location.RowIndexInSection, requestedId);
            return true;
        }

        if (!TryFindRowLocationById(key, out var found))
        {
            handle = default;
            return false;
        }

        handle = new RowHandle(found.SectionIndex, found.RowIndexInSection, requestedId);
        return true;
    }

    /// <inheritdoc />
    public bool TryGetDenseStringTableIndex(RowHandle row, int fieldIndex, out int stringTableIndex)
    {
        EnsureDenseStringTableMaterialized();

        // For dense-mode strings, the stored value is an int offset into the string block.
        // For non-string fields (or sparse mode), this should fail so callers fall back.
        if (Header.Flags.HasFlag(Db2Flags.Sparse))
        {
            stringTableIndex = 0;
            return false;
        }

        if (fieldIndex < 0)
        {
            stringTableIndex = 0;
            return false;
        }

        if ((uint)fieldIndex >= (uint)Header.FieldsCount)
            throw new ArgumentOutOfRangeException(nameof(fieldIndex));

        if ((uint)row.SectionIndex >= (uint)ParsedSections.Count)
            throw new ArgumentException("Invalid section index in RowHandle.", nameof(row));

        var section = ParsedSections[row.SectionIndex];
        if ((uint)row.RowIndexInSection >= (uint)section.NumRecords)
            throw new ArgumentException("Invalid row index in RowHandle.", nameof(row));

        var globalRowIndex = section.FirstGlobalRecordIndex + row.RowIndexInSection;

        var offset = ReadField<int>(row, fieldIndex);
        switch (offset)
        {
            case < 0:
                stringTableIndex = 0;
                return false;
            case 0:
                // Empty string (points to the section's string-table base null terminator).
                stringTableIndex = section.StringTableBaseOffset;
                return true;
        }

        var recordOffset = (long)(globalRowIndex * Header.RecordSize) - RecordsBlobSizeBytes;
        var fieldStartBytes = (long)(ColumnMeta[fieldIndex].RecordOffset >> 3);
        var stringIndex = recordOffset + fieldStartBytes + offset;

        if (stringIndex < 0)
            stringIndex = 0;

        if (stringIndex < section.StringTableBaseOffset)
        {
            stringTableIndex = 0;
            return false;
        }

        var sectionEndExclusive = section.StringTableBaseOffset + section.Header.StringTableSize;
        if (stringIndex >= sectionEndExclusive || stringIndex is > int.MaxValue)
        {
            stringTableIndex = 0;
            return false;
        }

        // Db2DenseStringScanner works in terms of start indices into DenseStringTableBytes.
        stringTableIndex = (int)stringIndex;
        return true;
    }

    private void EnsureDenseStringTableMaterialized()
    {
        if (!_denseStringTableBytes.IsEmpty || Header.StringTableSize <= 0)
            return;

        // Dense string table scanning is only meaningful for non-sparse files.
        if (Header.Flags.HasFlag(Db2Flags.Sparse))
            return;

        var denseStringTableBytes = new byte[Header.StringTableSize];
        var denseStringWriteOffset = 0;

        // IMPORTANT: StringTableBaseOffset is computed across ALL section headers (including
        // encrypted sections we may skip for record parsing). To keep offsets stable, we must
        // read per-section string tables for every raw section.
        for (var i = 0; i < Sections.Count; i++)
        {
            var section = Sections[i];
            if (section.StringTableSize <= 0)
                continue;

            if (denseStringWriteOffset + section.StringTableSize > denseStringTableBytes.Length)
                throw new InvalidDataException("WDC5 dense string table overflow: section string table exceeds declared size.");

            var recordDataSizeBytes = checked(section.NumRecords * Header.RecordSize);
            var stringTableOffset = checked((long)section.FileOffset + recordDataSizeBytes);

            _stream.Position = stringTableOffset;
            ReadExactly(_stream, denseStringTableBytes, start: denseStringWriteOffset, count: section.StringTableSize);
            denseStringWriteOffset += section.StringTableSize;
        }

        _denseStringTableBytes = new ReadOnlyMemory<byte>(denseStringTableBytes);
    }

    /// <inheritdoc />
    public bool TryGetRowById<TId>(TId id, out RowHandle row) where TId : IEquatable<TId>, IComparable<TId>
        => TryGetRowHandle(id, out row);

    /// <inheritdoc />
    public IEnumerable<RowHandle> EnumerateRows()
        => EnumerateRowHandles();

    private void EnsureMaterializedForEnumeration()
    {
        if (Header.Flags.HasFlag(Db2Flags.Sparse))
        {
            // Sparse inline strings and variable records cannot be enumerated without record bytes.
            // Materialize on demand.
        }

        // If any parsed section already has record bytes, assume fully materialized.
        if (ParsedSections is { Count: > 0 } && ParsedSections[0].RecordsData is not null)
            return;

        if (Options.RecordLoadingMode == Wdc5RecordLoadingMode.Eager)
            return;

        var denseStringTableBytes = Header.StringTableSize > 0 ? new byte[Header.StringTableSize] : [];
        var denseStringWriteOffset = 0;

        for (var i = 0; i < ParsedSections.Count; i++)
        {
            var section = ParsedSections[i];

            _reader.BaseStream.Position = section.Header.FileOffset;

            if (!Header.Flags.HasFlag(Db2Flags.Sparse))
            {
                var recordDataSizeBytes = section.RecordDataSizeBytes;
                var recordsData = new byte[checked(recordDataSizeBytes + 8)];
                ReadExactly(_reader, recordsData, recordDataSizeBytes);

                if (section.Header.StringTableSize > 0)
                {
                    if (denseStringWriteOffset + section.Header.StringTableSize > denseStringTableBytes.Length)
                        throw new InvalidDataException("WDC5 dense string table overflow: section string table exceeds declared size.");

                    ReadExactly(_reader, denseStringTableBytes, start: denseStringWriteOffset, count: section.Header.StringTableSize);
                    denseStringWriteOffset += section.Header.StringTableSize;
                }

                section.RecordsData = recordsData;
            }
            else
            {
                var recordDataSizeBytes = section.RecordDataSizeBytes;
                var recordsData = new byte[checked(recordDataSizeBytes + 8)];
                ReadExactly(_reader, recordsData, recordDataSizeBytes);

                if (_reader.BaseStream.Position != section.Header.OffsetRecordsEndOffset)
                    throw new InvalidDataException("WDC5 sparse section parsing desynced: expected OffsetRecordsEndOffset.");

                section.RecordsData = recordsData;
            }
        }

        _denseStringTableBytes = new ReadOnlyMemory<byte>(denseStringTableBytes);
    }

    private bool TryResolveCopySourceId(int destinationId, out int sourceId)
    {
        for (var sectionIndex = 0; sectionIndex < ParsedSections.Count; sectionIndex++)
        {
            var section = ParsedSections[sectionIndex];
            if (section.CopyData.TryGetValue(destinationId, out sourceId))
                return true;
        }

        sourceId = 0;
        return false;
    }

    private bool TryFindRowLocationById(int id, out (int SectionIndex, int RowIndexInSection, int GlobalRecordIndex) location)
    {
        for (var sectionIndex = 0; sectionIndex < ParsedSections.Count; sectionIndex++)
        {
            var section = ParsedSections[sectionIndex];

            // Prefer IndexData when present; this avoids reading record bytes.
            if (section.IndexData is { Length: not 0 })
            {
                var indexData = section.IndexData;
                for (var rowIndex = 0; rowIndex < section.NumRecords; rowIndex++)
                {
                    if (indexData[rowIndex] != id)
                        continue;

                    location = (sectionIndex, rowIndex, section.FirstGlobalRecordIndex + rowIndex);
                    return true;
                }

                continue;
            }

            // Fallback: decode physical ID field from record bits (may require reading row bytes lazily).
            for (var rowIndex = 0; rowIndex < section.NumRecords; rowIndex++)
            {
                if (GetVirtualIdForIndexing(sectionIndex, section, rowIndex) != id)
                    continue;

                location = (sectionIndex, rowIndex, section.FirstGlobalRecordIndex + rowIndex);
                return true;
            }
        }

        location = default;
        return false;
    }

    private void EnsureIdIndexBuilt()
    {
        if (_idIndex is not null)
            return;

        var idIndex = new Dictionary<int, (int SectionIndex, int RowIndexInSection, int GlobalRecordIndex)>(capacity: Header.RecordsCount);

        for (var sectionIndex = 0; sectionIndex < ParsedSections.Count; sectionIndex++)
        {
            var section = ParsedSections[sectionIndex];

            // Prefer IndexData when present; this avoids reading record bytes.
            if (section.IndexData is { Length: not 0 })
            {
                var indexData = section.IndexData;
                for (var rowIndex = 0; rowIndex < section.NumRecords; rowIndex++)
                {
                    var id = indexData[rowIndex];
                    if (id != -1)
                        idIndex.TryAdd(id, (sectionIndex, rowIndex, section.FirstGlobalRecordIndex + rowIndex));
                }

                continue;
            }

            // Fallback: decode physical ID field from record bits (may require reading row bytes lazily).
            for (var rowIndex = 0; rowIndex < section.NumRecords; rowIndex++)
            {
                var id = GetVirtualIdForIndexing(sectionIndex, section, rowIndex);
                if (id != -1)
                    idIndex.TryAdd(id, (sectionIndex, rowIndex, section.FirstGlobalRecordIndex + rowIndex));
            }
        }

        _idIndex = idIndex;
    }

    private int GetVirtualIdForIndexing(int sectionIndex, Wdc5Section section, int rowIndex)
    {
        if (Header.IdFieldIndex >= Header.FieldsCount)
            return section.FirstGlobalRecordIndex + rowIndex;

        if (section.IndexData is { Length: not 0 })
            return section.IndexData[rowIndex];

        // Read just this row's bytes and decode up to the ID field.
        var rowBytes = GetRowBytesFromStream(sectionIndex, section, rowIndex, out _, out var rowStartBitOffset);
        var tmp = new Wdc5RowReader(rowBytes, positionBits: rowStartBitOffset);

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

            Wdc5FieldDecoder.ReadScalar<uint>(id: 0, ref tmp, fieldMeta, columnMeta, palletData, commonData);
        }
        return -1;
    }

    private Wdc5RowReader CreateReaderAtRowStart(Wdc5Section section, int rowIndex, out ReadOnlySpan<byte> recordBytes, out int rowStartByte, out int rowSizeBytes)
    {
        if (section.RecordsData is null)
            throw new InvalidOperationException("Record bytes are not materialized for this section.");

        if (!Header.Flags.HasFlag(Db2Flags.Sparse))
        {
            recordBytes = section.RecordsData;
            rowStartByte = rowIndex * Header.RecordSize;
            rowSizeBytes = Header.RecordSize;
            return new Wdc5RowReader(recordBytes, positionBits: rowStartByte * 8);
        }

        if (section.SparseRecordStartBits is { Length: 0 })
            throw new InvalidDataException("Sparse WDC5 section missing SparseRecordStartBits.");

        var startBits = section.SparseRecordStartBits[rowIndex];
        var startBytes = startBits >> 3;
        var sizeBytes = (int)section.SparseEntries[rowIndex].Size;
        var endExclusive = (long)startBytes + sizeBytes;
        if (startBytes < 0 || endExclusive < 0 || endExclusive > section.RecordsData.Length)
            throw new InvalidDataException("Sparse WDC5 row points outside section record data.");

        recordBytes = section.RecordsData;
        rowStartByte = startBytes;
        rowSizeBytes = sizeBytes;
        return new Wdc5RowReader(recordBytes, positionBits: startBits);
    }

    private int GetVirtualId(Wdc5Section section, int rowIndex, Wdc5RowReader readerAtStart)
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

            Wdc5FieldDecoder.ReadScalar<uint>(id: 0, ref tmp, fieldMeta, columnMeta, palletData, commonData);
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

    /// <inheritdoc />
    public T ReadField<T>(RowHandle handle, int fieldIndex)
    {
        if ((uint)handle.SectionIndex >= (uint)ParsedSections.Count)
            throw new ArgumentException("Invalid section index in RowHandle.", nameof(handle));

        var section = ParsedSections[handle.SectionIndex];
        if ((uint)handle.RowIndexInSection >= (uint)section.NumRecords)
            throw new ArgumentException("Invalid row index in RowHandle.", nameof(handle));

        Wdc5RowReader readerAtStart;
        ReadOnlySpan<byte> recordBytes;
        int rowEndExclusive;
        int sourceId;

        if (section.RecordsData is not null)
        {
            readerAtStart = CreateReaderAtRowStart(section, handle.RowIndexInSection, out recordBytes, out var rowStartByte, out var rowSizeBytes);
            sourceId = GetVirtualId(section, handle.RowIndexInSection, readerAtStart);
            rowEndExclusive = rowStartByte + rowSizeBytes;

            var destinationId2 = handle.RowId;
            var nonceId2 = Options.EncryptedRowNonceStrategy == Wdc5EncryptedRowNonceStrategy.SourceId ? sourceId : destinationId2;

            var globalRowIndex2 = section.FirstGlobalRecordIndex + handle.RowIndexInSection;
            var referenceKey2 = Header.Flags.HasFlag(Db2Flags.SecondaryKey) && section.IndexData is { Length: not 0 }
                ? section.IndexData[handle.RowIndexInSection]
                : handle.RowIndexInSection;

            section.ParentLookupEntries.TryGetValue(referenceKey2, out var parentRelationId2);

            if (section.IsDecryptable)
            {
                var ciphertext = recordBytes.Slice(rowStartByte, rowSizeBytes);
                using var decrypted = DecryptRowBytes(ciphertext, nonceId2, section.TactKey.Span);
                var rowStartBitOffset2 = readerAtStart.PositionBits - (rowStartByte * 8);
                var decryptedReaderAtStart = new Wdc5RowReader(decrypted.Bytes, positionBits: rowStartBitOffset2);
                return ReadFieldTyped<T>(section, handle.RowIndexInSection, decryptedReaderAtStart, decrypted.Bytes, rowEndExclusive: rowSizeBytes, globalRowIndex2, sourceId, destinationId2, parentRelationId2, fieldIndex);
            }

            return ReadFieldTyped<T>(section, handle.RowIndexInSection, readerAtStart, recordBytes, rowEndExclusive, globalRowIndex2, sourceId, destinationId2, parentRelationId2, fieldIndex);
        }

        recordBytes = GetRowBytesFromStream(handle.SectionIndex, section, handle.RowIndexInSection, out var streamedRowSizeBytes, out var streamedRowStartBitOffset);
        readerAtStart = new Wdc5RowReader(recordBytes, positionBits: streamedRowStartBitOffset);
        sourceId = GetVirtualId(section, handle.RowIndexInSection, readerAtStart);
        rowEndExclusive = streamedRowSizeBytes;

        var destinationId = handle.RowId;
        var nonceId = Options.EncryptedRowNonceStrategy == Wdc5EncryptedRowNonceStrategy.SourceId ? sourceId : destinationId;

        var globalRowIndex = section.FirstGlobalRecordIndex + handle.RowIndexInSection;

        var referenceKey = Header.Flags.HasFlag(Db2Flags.SecondaryKey) && section.IndexData is { Length: not 0 }
            ? section.IndexData[handle.RowIndexInSection]
            : handle.RowIndexInSection;

        section.ParentLookupEntries.TryGetValue(referenceKey, out var parentRelationId);

        if (section.IsDecryptable)
        {
            using var decrypted = DecryptRowBytes(recordBytes[..streamedRowSizeBytes], nonceId, section.TactKey.Span);
            var decryptedReaderAtStart = new Wdc5RowReader(decrypted.Bytes, positionBits: streamedRowStartBitOffset);
            return ReadFieldTyped<T>(section, handle.RowIndexInSection, decryptedReaderAtStart, decrypted.Bytes, rowEndExclusive: streamedRowSizeBytes, globalRowIndex, sourceId, destinationId, parentRelationId, fieldIndex);
        }

        return ReadFieldTyped<T>(section, handle.RowIndexInSection, readerAtStart, recordBytes, rowEndExclusive, globalRowIndex, sourceId, destinationId, parentRelationId, fieldIndex);
    }

    private ReadOnlySpan<byte> GetRowBytesFromStream(int sectionIndex, Wdc5Section section, int rowIndexInSection, out int rowSizeBytes, out int rowStartBitOffset)
    {
        if (_cachedRowValid && _cachedSectionIndex == sectionIndex && _cachedRowIndexInSection == rowIndexInSection)
        {
            rowSizeBytes = _cachedRowSizeBytes;
            rowStartBitOffset = _cachedRowStartBitOffset;
            return _cachedRowBytes.AsSpan(0, rowSizeBytes);
        }

        if (!Header.Flags.HasFlag(Db2Flags.Sparse))
        {
            rowSizeBytes = Header.RecordSize;
            rowStartBitOffset = 0;

            EnsureCachedRowBufferCapacity(rowSizeBytes);

            var fileOffset = checked((long)section.Header.FileOffset + (long)rowIndexInSection * Header.RecordSize);
            _stream.Position = fileOffset;
            ReadExactly(_stream, _cachedRowBytes!, start: 0, count: rowSizeBytes);
            Array.Clear(_cachedRowBytes!, rowSizeBytes, 8);

            _cachedRowSizeBytes = rowSizeBytes;
            _cachedRowStartBitOffset = rowStartBitOffset;
            _cachedSectionIndex = sectionIndex;
            _cachedRowIndexInSection = rowIndexInSection;
            _cachedRowValid = true;
            return _cachedRowBytes.AsSpan(0, rowSizeBytes);
        }

        if (section.SparseEntries is { Length: 0 } || section.SparseRecordStartBits is { Length: 0 })
            throw new InvalidDataException("Sparse WDC5 section missing sparse metadata.");

        var entry = section.SparseEntries[rowIndexInSection];
        var startBits = section.SparseRecordStartBits[rowIndexInSection];
        var startBytes = startBits >> 3;
        rowStartBitOffset = (int)(startBits & 7);
        rowSizeBytes = checked((int)entry.Size);

        EnsureCachedRowBufferCapacity(rowSizeBytes);

        var fileOffset2 = checked((long)section.Header.FileOffset + startBytes);
        _stream.Position = fileOffset2;
        ReadExactly(_stream, _cachedRowBytes!, start: 0, count: rowSizeBytes);
        Array.Clear(_cachedRowBytes!, rowSizeBytes, 8);

        _cachedRowSizeBytes = rowSizeBytes;
        _cachedRowStartBitOffset = rowStartBitOffset;
        _cachedSectionIndex = sectionIndex;
        _cachedRowIndexInSection = rowIndexInSection;
        _cachedRowValid = true;
        return _cachedRowBytes.AsSpan(0, rowSizeBytes);
    }

    private void EnsureCachedRowBufferCapacity(int rowSizeBytes)
    {
        var required = checked(rowSizeBytes + 8);
        if (_cachedRowBytes is null || _cachedRowBytes.Length < required)
            _cachedRowBytes = new byte[required];
    }

    private static void ReadExactly(Stream stream, byte[] buffer, int start, int count)
    {
        var totalRead = 0;
        while (totalRead < count)
        {
            var read = stream.Read(buffer, start + totalRead, count - totalRead);
            if (read <= 0)
                throw new EndOfStreamException("Unexpected end of stream while reading WDC5 row bytes.");
            totalRead += read;
        }
    }

    private T ReadFieldTyped<T>(
        Wdc5Section section,
        int rowIndexInSection,
        Wdc5RowReader readerAtStart,
        ReadOnlySpan<byte> recordBytes,
        int rowEndExclusive,
        int globalRowIndex,
        int sourceId,
        int destinationId,
        int parentRelationId,
        int fieldIndex)
    {
        var type = typeof(T);

        if (type.IsEnum)
            return (T)ReadFieldBoxedFromPrepared(section, rowIndexInSection, readerAtStart, recordBytes, rowEndExclusive, globalRowIndex, sourceId, destinationId, parentRelationId, Enum.GetUnderlyingType(type), fieldIndex);

        if (fieldIndex < 0)
        {
            return fieldIndex switch
            {
                Db2VirtualFieldIndex.Id => CastVirtualField<T>(destinationId),
                Db2VirtualFieldIndex.ParentRelation => CastVirtualField<T>(parentRelationId),
                _ => throw new NotSupportedException($"Unsupported virtual field index {fieldIndex}."),
            };
        }

        if ((uint)fieldIndex >= (uint)Header.FieldsCount)
            throw new ArgumentOutOfRangeException(nameof(fieldIndex));

        if (type == typeof(string))
        {
            TryGetString(section, rowIndexInSection, globalRowIndex, readerAtStart, recordBytes, rowEndExclusive, sourceId, fieldIndex, out var s);
            return Unsafe.As<string, T>(ref s);
        }

        if (type.IsArray)
        {
            var elementType = type.GetElementType()!;
            if (elementType == typeof(double))
            {
                var floats = ReadArray<float>(readerAtStart, fieldIndex);
                var doubles = new double[floats.Length];
                for (var i = 0; i < floats.Length; i++)
                    doubles[i] = floats[i];
                return Unsafe.As<double[], T>(ref doubles);
            }

            return (T)GetArrayBoxed(readerAtStart, elementType, fieldIndex);
        }

        var localReader = MoveToFieldStart(fieldIndex, readerAtStart);
        ref readonly var fieldMeta = ref FieldMeta[fieldIndex];
        ref readonly var columnMeta = ref ColumnMeta[fieldIndex];
        var palletData = PalletData[fieldIndex];
        var commonData = CommonData[fieldIndex];

        return ReadScalarTyped<T>(sourceId, ref localReader, fieldMeta, columnMeta, palletData, commonData);
    }

    private static T CastVirtualField<T>(int value)
    {
        if (typeof(T) == typeof(int))
            return Unsafe.As<int, T>(ref value);
        if (typeof(T) == typeof(uint))
        {
            var u = unchecked((uint)value);
            return Unsafe.As<uint, T>(ref u);
        }
        if (typeof(T) == typeof(long))
        {
            var l = (long)value;
            return Unsafe.As<long, T>(ref l);
        }
        if (typeof(T) == typeof(ulong))
        {
            var ul = unchecked((ulong)(uint)value);
            return Unsafe.As<ulong, T>(ref ul);
        }
        if (typeof(T) == typeof(short))
        {
            var s = checked((short)value);
            return Unsafe.As<short, T>(ref s);
        }
        if (typeof(T) == typeof(ushort))
        {
            var us = checked((ushort)value);
            return Unsafe.As<ushort, T>(ref us);
        }
        if (typeof(T) == typeof(byte))
        {
            var b = checked((byte)value);
            return Unsafe.As<byte, T>(ref b);
        }
        if (typeof(T) == typeof(sbyte))
        {
            var sb = checked((sbyte)value);
            return Unsafe.As<sbyte, T>(ref sb);
        }

        return (T)Convert.ChangeType(value, typeof(T));
    }

    private static T ReadScalarTyped<T>(int id, ref Wdc5RowReader reader, FieldMetaData fieldMeta, ColumnMetaData columnMeta, uint[] palletData, Dictionary<int, uint> commonData)
    {
        if (typeof(T) == typeof(byte))
        {
            var v = Wdc5FieldDecoder.ReadScalar<byte>(id, ref reader, fieldMeta, columnMeta, palletData, commonData);
            return Unsafe.As<byte, T>(ref v);
        }
        if (typeof(T) == typeof(sbyte))
        {
            var v = Wdc5FieldDecoder.ReadScalar<sbyte>(id, ref reader, fieldMeta, columnMeta, palletData, commonData);
            return Unsafe.As<sbyte, T>(ref v);
        }
        if (typeof(T) == typeof(short))
        {
            var v = Wdc5FieldDecoder.ReadScalar<short>(id, ref reader, fieldMeta, columnMeta, palletData, commonData);
            return Unsafe.As<short, T>(ref v);
        }
        if (typeof(T) == typeof(ushort))
        {
            var v = Wdc5FieldDecoder.ReadScalar<ushort>(id, ref reader, fieldMeta, columnMeta, palletData, commonData);
            return Unsafe.As<ushort, T>(ref v);
        }
        if (typeof(T) == typeof(int))
        {
            var v = Wdc5FieldDecoder.ReadScalar<int>(id, ref reader, fieldMeta, columnMeta, palletData, commonData);
            return Unsafe.As<int, T>(ref v);
        }
        if (typeof(T) == typeof(uint))
        {
            var v = Wdc5FieldDecoder.ReadScalar<uint>(id, ref reader, fieldMeta, columnMeta, palletData, commonData);
            return Unsafe.As<uint, T>(ref v);
        }
        if (typeof(T) == typeof(long))
        {
            var v = Wdc5FieldDecoder.ReadScalar<long>(id, ref reader, fieldMeta, columnMeta, palletData, commonData);
            return Unsafe.As<long, T>(ref v);
        }
        if (typeof(T) == typeof(ulong))
        {
            var v = Wdc5FieldDecoder.ReadScalar<ulong>(id, ref reader, fieldMeta, columnMeta, palletData, commonData);
            return Unsafe.As<ulong, T>(ref v);
        }
        if (typeof(T) == typeof(float))
        {
            var v = Wdc5FieldDecoder.ReadScalar<float>(id, ref reader, fieldMeta, columnMeta, palletData, commonData);
            return Unsafe.As<float, T>(ref v);
        }
        if (typeof(T) == typeof(double))
        {
            var f = Wdc5FieldDecoder.ReadScalar<float>(id, ref reader, fieldMeta, columnMeta, palletData, commonData);
            var d = (double)f;
            return Unsafe.As<double, T>(ref d);
        }

        throw new NotSupportedException($"Unsupported scalar type {typeof(T).FullName}.");
    }

    /// <summary>
    /// Reads all fields for a row into a caller-provided buffer.
    /// </summary>
    /// <param name="handle">A row handle obtained from this file.</param>
    /// <param name="values">
    /// Destination buffer. Must contain at least <c>Header.FieldsCount + 2</c> elements,
    /// where the first two slots are reserved for virtual fields (ID and parent relation).
    /// </param>
    public void ReadAllFields(RowHandle handle, Span<object> values)
    {
        if (values.Length < Header.FieldsCount + 2)
            throw new ArgumentException($"Values span must have at least {Header.FieldsCount + 2} elements.");

        if ((uint)handle.SectionIndex >= (uint)ParsedSections.Count)
            throw new ArgumentException("Invalid section index in RowHandle.", nameof(handle));

        var section = ParsedSections[handle.SectionIndex];
        if ((uint)handle.RowIndexInSection >= (uint)section.NumRecords)
            throw new ArgumentException("Invalid row index in RowHandle.", nameof(handle));

        Wdc5RowReader readerAtStart;
        ReadOnlySpan<byte> recordBytes;
        int rowStartByte;
        int rowSizeBytes;

        if (section.RecordsData is not null)
        {
            readerAtStart = CreateReaderAtRowStart(section, handle.RowIndexInSection, out recordBytes, out rowStartByte, out rowSizeBytes);
        }
        else
        {
            recordBytes = GetRowBytesFromStream(handle.SectionIndex, section, handle.RowIndexInSection, out rowSizeBytes, out var rowStartBitOffset);
            rowStartByte = 0;
            readerAtStart = new Wdc5RowReader(recordBytes, positionBits: rowStartBitOffset);
        }

        var sourceId = GetVirtualId(section, handle.RowIndexInSection, readerAtStart);
        var destinationId = handle.RowId;
        var nonceId = Options.EncryptedRowNonceStrategy == Wdc5EncryptedRowNonceStrategy.SourceId ? sourceId : destinationId;

        var globalRowIndex = section.FirstGlobalRecordIndex + handle.RowIndexInSection;

        var referenceKey = Header.Flags.HasFlag(Db2Flags.SecondaryKey) && section.IndexData is { Length: not 0 }
            ? section.IndexData[handle.RowIndexInSection]
            : handle.RowIndexInSection;

        section.ParentLookupEntries.TryGetValue(referenceKey, out var parentRelationId);

        if (section.IsDecryptable)
        {
            var ciphertext = recordBytes.Slice(rowStartByte, rowSizeBytes);
            using var decrypted = DecryptRowBytes(ciphertext, nonceId, section.TactKey.Span);
            var rowStartBitOffset = readerAtStart.PositionBits - (rowStartByte * 8);
            var decryptedReaderAtStart = new Wdc5RowReader(decrypted.Bytes, positionBits: rowStartBitOffset);

            values[0] = ReadFieldBoxedFromPrepared(section, handle.RowIndexInSection, decryptedReaderAtStart, decrypted.Bytes, rowEndExclusive: rowSizeBytes, globalRowIndex, sourceId, destinationId, parentRelationId, type: typeof(int), fieldIndex: Db2VirtualFieldIndex.Id);
            values[1] = ReadFieldBoxedFromPrepared(section, handle.RowIndexInSection, decryptedReaderAtStart, decrypted.Bytes, rowEndExclusive: rowSizeBytes, globalRowIndex, sourceId, destinationId, parentRelationId, type: typeof(int), fieldIndex: Db2VirtualFieldIndex.ParentRelation);

            for (var i = 0; i < Header.FieldsCount; i++)
                values[i + 2] = ReadFieldBoxedFromPrepared(section, handle.RowIndexInSection, decryptedReaderAtStart, decrypted.Bytes, rowEndExclusive: rowSizeBytes, globalRowIndex, sourceId, destinationId, parentRelationId, type: typeof(object), fieldIndex: i);

            return;
        }

        var rowEndExclusive = rowStartByte + rowSizeBytes;
        values[0] = ReadFieldBoxedFromPrepared(section, handle.RowIndexInSection, readerAtStart, recordBytes, rowEndExclusive, globalRowIndex, sourceId, destinationId, parentRelationId, type: typeof(int), fieldIndex: Db2VirtualFieldIndex.Id);
        values[1] = ReadFieldBoxedFromPrepared(section, handle.RowIndexInSection, readerAtStart, recordBytes, rowEndExclusive, globalRowIndex, sourceId, destinationId, parentRelationId, type: typeof(int), fieldIndex: Db2VirtualFieldIndex.ParentRelation);

        for (var i = 0; i < Header.FieldsCount; i++)
            values[i + 2] = ReadFieldBoxedFromPrepared(section, handle.RowIndexInSection, readerAtStart, recordBytes, rowEndExclusive, globalRowIndex, sourceId, destinationId, parentRelationId, type: typeof(object), fieldIndex: i);
    }

    private object ReadFieldBoxed(RowHandle handle, Type type, int fieldIndex)
    {
        if ((uint)handle.SectionIndex >= (uint)ParsedSections.Count)
            throw new ArgumentException("Invalid section index in RowHandle.", nameof(handle));

        var section = ParsedSections[handle.SectionIndex];
        if ((uint)handle.RowIndexInSection >= (uint)section.NumRecords)
            throw new ArgumentException("Invalid row index in RowHandle.", nameof(handle));

        Wdc5RowReader readerAtStart;
        ReadOnlySpan<byte> recordBytes;
        int rowStartByte;
        int rowSizeBytes;

        if (section.RecordsData is not null)
        {
            readerAtStart = CreateReaderAtRowStart(section, handle.RowIndexInSection, out recordBytes, out rowStartByte, out rowSizeBytes);
        }
        else
        {
            recordBytes = GetRowBytesFromStream(handle.SectionIndex, section, handle.RowIndexInSection, out rowSizeBytes, out var rowStartBitOffset);
            rowStartByte = 0;
            readerAtStart = new Wdc5RowReader(recordBytes, positionBits: rowStartBitOffset);
        }

        var sourceId = GetVirtualId(section, handle.RowIndexInSection, readerAtStart);
        var destinationId = handle.RowId;
        var nonceId = Options.EncryptedRowNonceStrategy == Wdc5EncryptedRowNonceStrategy.SourceId ? sourceId : destinationId;

        var globalRowIndex = section.FirstGlobalRecordIndex + handle.RowIndexInSection;

        var referenceKey = Header.Flags.HasFlag(Db2Flags.SecondaryKey) && section.IndexData is { Length: not 0 }
            ? section.IndexData[handle.RowIndexInSection]
            : handle.RowIndexInSection;

        section.ParentLookupEntries.TryGetValue(referenceKey, out var parentRelationId);

        if (section.IsDecryptable)
        {
            var ciphertext = recordBytes.Slice(rowStartByte, rowSizeBytes);
            using var decrypted = DecryptRowBytes(ciphertext, nonceId, section.TactKey.Span);
            var rowStartBitOffset = readerAtStart.PositionBits - (rowStartByte * 8);
            var decryptedReaderAtStart = new Wdc5RowReader(decrypted.Bytes, positionBits: rowStartBitOffset);
            return ReadFieldBoxedFromPrepared(section, handle.RowIndexInSection, decryptedReaderAtStart, decrypted.Bytes, rowEndExclusive: rowSizeBytes, globalRowIndex, sourceId, destinationId, parentRelationId, type, fieldIndex);
        }

        var rowEndExclusive = rowStartByte + rowSizeBytes;
        return ReadFieldBoxedFromPrepared(section, handle.RowIndexInSection, readerAtStart, recordBytes, rowEndExclusive, globalRowIndex, sourceId, destinationId, parentRelationId, type, fieldIndex);
    }

    private object ReadFieldBoxedFromPrepared(
        Wdc5Section section,
        int rowIndexInSection,
        Wdc5RowReader readerAtStart,
        ReadOnlySpan<byte> recordBytes,
        int rowEndExclusive,
        int globalRowIndex,
        int sourceId,
        int destinationId,
        int parentRelationId,
        Type type,
        int fieldIndex)
    {
        if (type.IsEnum)
            type = Enum.GetUnderlyingType(type);

        if (fieldIndex < 0)
        {
            return fieldIndex switch
            {
                Db2VirtualFieldIndex.Id => Convert.ChangeType(destinationId, type),
                Db2VirtualFieldIndex.ParentRelation => Convert.ChangeType(parentRelationId, type),
                _ => throw new NotSupportedException($"Unsupported virtual field index {fieldIndex}."),
            };
        }

        if ((uint)fieldIndex >= (uint)Header.FieldsCount)
            throw new ArgumentOutOfRangeException(nameof(fieldIndex));

        if (type == typeof(string))
        {
            TryGetString(section, rowIndexInSection, globalRowIndex, readerAtStart, recordBytes, rowEndExclusive, sourceId, fieldIndex, out var s);
            return s;
        }

        if (type == typeof(object))
        {
            ref readonly var fieldMeta = ref FieldMeta[fieldIndex];
            if (fieldMeta.Bits == 0)
            {
                TryGetString(section, rowIndexInSection, globalRowIndex, readerAtStart, recordBytes, rowEndExclusive, sourceId, fieldIndex, out var s);
                return s;
            }

            type = typeof(int);
        }

        if (type.IsArray)
        {
            var elementType = type.GetElementType()!;
            if (elementType == typeof(double))
            {
                var floats = ReadArray<float>(readerAtStart, fieldIndex);
                var doubles = new double[floats.Length];
                for (var i = 0; i < floats.Length; i++)
                    doubles[i] = floats[i];
                return doubles;
            }

            return GetArrayBoxed(readerAtStart, elementType, fieldIndex);
        }

        var localReader = MoveToFieldStart(fieldIndex, readerAtStart);
        ref readonly var fieldMeta2 = ref FieldMeta[fieldIndex];
        ref readonly var columnMeta2 = ref ColumnMeta[fieldIndex];
        var palletData2 = PalletData[fieldIndex];
        var commonData2 = CommonData[fieldIndex];
        return ReadScalarBoxed(type, sourceId, ref localReader, fieldMeta2, columnMeta2, palletData2, commonData2);
    }

    private static object ReadScalarBoxed(Type type, int id, ref Wdc5RowReader reader, FieldMetaData fieldMeta, ColumnMetaData columnMeta, uint[] palletData, Dictionary<int, uint> commonData)
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

    private object GetArrayBoxed(Wdc5RowReader readerAtStart, Type elementType, int fieldIndex)
    {
        if (elementType == typeof(byte))
            return ReadArray<byte>(readerAtStart, fieldIndex);
        if (elementType == typeof(sbyte))
            return ReadArray<sbyte>(readerAtStart, fieldIndex);
        if (elementType == typeof(short))
            return ReadArray<short>(readerAtStart, fieldIndex);
        if (elementType == typeof(ushort))
            return ReadArray<ushort>(readerAtStart, fieldIndex);
        if (elementType == typeof(int))
            return ReadArray<int>(readerAtStart, fieldIndex);
        if (elementType == typeof(uint))
            return ReadArray<uint>(readerAtStart, fieldIndex);
        if (elementType == typeof(long))
            return ReadArray<long>(readerAtStart, fieldIndex);
        if (elementType == typeof(ulong))
            return ReadArray<ulong>(readerAtStart, fieldIndex);
        if (elementType == typeof(float))
            return ReadArray<float>(readerAtStart, fieldIndex);

        throw new NotSupportedException($"Unsupported array element type {elementType.FullName}.");
    }

    private T[] ReadArray<T>(Wdc5RowReader readerAtStart, int fieldIndex) where T : unmanaged
    {
        if ((uint)fieldIndex >= (uint)Header.FieldsCount)
            throw new ArgumentOutOfRangeException(nameof(fieldIndex));

        var localReader2 = MoveToFieldStart(fieldIndex, readerAtStart);
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

    private bool TryGetString(Wdc5Section section, int rowIndexInSection, int globalRowIndex, Wdc5RowReader readerAtStart, ReadOnlySpan<byte> recordBytes, int rowEndExclusive, int id, int fieldIndex, out string value)
    {
        if ((uint)fieldIndex >= (uint)Header.FieldsCount)
            throw new ArgumentOutOfRangeException(nameof(fieldIndex));
        return TryGetDenseString(section, globalRowIndex, readerAtStart, id, fieldIndex, out value) ||
               TryGetInlineString(section, rowIndexInSection, readerAtStart, recordBytes, rowEndExclusive, id, fieldIndex, out value);
    }

    private bool TryGetDenseString(Wdc5Section section, int globalRowIndex, Wdc5RowReader readerAtStart, int id, int fieldIndex, out string value)
    {
        if (Header.Flags.HasFlag(Db2Flags.Sparse))
        {
            value = string.Empty;
            return false;
        }

        if ((uint)fieldIndex >= (uint)Header.FieldsCount)
            throw new ArgumentOutOfRangeException(nameof(fieldIndex));

        var localReader2 = MoveToFieldStart(fieldIndex, readerAtStart);
        ref readonly var fieldMeta2 = ref FieldMeta[fieldIndex];
        ref readonly var columnMeta2 = ref ColumnMeta[fieldIndex];

        if (columnMeta2 is { CompressionType: CompressionType.PalletArray, Pallet.Cardinality: not 1 })
        {
            value = string.Empty;
            return false;
        }

        var offset = Wdc5FieldDecoder.ReadScalar<int>(id, ref localReader2, fieldMeta2, columnMeta2, PalletData[fieldIndex], CommonData[fieldIndex]);
        switch (offset)
        {
            case 0:
                value = string.Empty;
                return true;
            case < 0:
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

        if (!DenseStringTableBytes.IsEmpty)
            return TryReadNullTerminatedUtf8(DenseStringTableBytes.Span, startIndex: (int)stringIndex, endExclusive: sectionEndExclusive, out value);

        var fileOffset = checked((long)section.Header.FileOffset + section.RecordDataSizeBytes + (stringIndex - section.StringTableBaseOffset));
        var sectionStringsEnd = checked((long)section.Header.FileOffset + section.RecordDataSizeBytes + section.Header.StringTableSize);
        return TryReadNullTerminatedUtf8FromStream(_stream, fileOffset, sectionStringsEnd, out value);
    }

    private bool TryGetInlineString(Wdc5Section section, int rowIndexInSection, Wdc5RowReader readerAtStart, ReadOnlySpan<byte> recordBytes, int rowEndExclusive, int id, int fieldIndex, out string value)
    {
        if (!Header.Flags.HasFlag(Db2Flags.Sparse))
        {
            value = string.Empty;
            return false;
        }

        if ((uint)fieldIndex >= (uint)Header.FieldsCount)
            throw new ArgumentOutOfRangeException(nameof(fieldIndex));

        int fieldStart2;
        if (!section.IsDecryptable)
        {
            fieldStart2 = GetOrCreateSparseOffsetTable(section).GetFieldBitPosition(rowIndexInSection, fieldIndex) >> 3;
        }
        else
        {
            var localReader2 = readerAtStart;
            for (var i = 0; i < fieldIndex; i++)
                SkipSparseField(ref localReader2, i, recordBytes, rowEndExclusive, id);

            fieldStart2 = localReader2.PositionBits >> 3;
        }

        if (fieldStart2 >= 0 && fieldStart2 < rowEndExclusive && TryReadNullTerminatedUtf8(recordBytes, startIndex: fieldStart2, endExclusive: rowEndExclusive, out value))
            return true;

        value = string.Empty;
        return false;
    }

    private void SkipSparseField(ref Wdc5RowReader reader, int fieldIndex, ReadOnlySpan<byte> recordBytes, int endExclusive, int id)
    {
        ref readonly var fieldMeta = ref FieldMeta[fieldIndex];
        ref readonly var columnMeta = ref ColumnMeta[fieldIndex];

        switch (columnMeta)
        {
            case { CompressionType: CompressionType.None }:
                {
                    var bitSize = 32 - fieldMeta.Bits;
                    if (bitSize <= 0)
                        bitSize = columnMeta.Immediate.BitWidth;

                    if (bitSize == 32)
                    {
                        var currentBytePos = reader.PositionBits >> 3;
                        var terminatorIndex = recordBytes[currentBytePos..endExclusive].IndexOf((byte)0);
                        if (terminatorIndex >= 0)
                        {
                            reader.PositionBits += (terminatorIndex + 1) * 8;
                            return;
                        }
                    }

                    reader.PositionBits += bitSize;
                    break;
                }

            default:
                Wdc5FieldDecoder.ReadScalar<long>(id, ref reader, fieldMeta, columnMeta, PalletData[fieldIndex], CommonData[fieldIndex]);
                break;
        }
    }

    private static readonly UTF8Encoding Utf8Strict = new(encoderShouldEmitUTF8Identifier: false, throwOnInvalidBytes: true);

    internal static bool TryReadNullTerminatedUtf8(ReadOnlySpan<byte> bytes, int startIndex, int endExclusive, out string value)
    {
        if (startIndex < 0 || startIndex >= bytes.Length || endExclusive < 0 || endExclusive > bytes.Length || startIndex >= endExclusive)
        {
            value = string.Empty;
            return false;
        }

        var span = bytes[startIndex..endExclusive];
        var terminatorIndex = span.IndexOf((byte)0);
        switch (terminatorIndex)
        {
            case < 0:
                value = string.Empty;
                return false;
            case 0:
                value = string.Empty;
                return true;
        }

        try
        {
            value = Utf8Strict.GetString(span[..terminatorIndex]);
            return true;
        }
        catch (DecoderFallbackException)
        {
            value = string.Empty;
            return false;
        }
    }

    private static bool TryReadNullTerminatedUtf8FromStream(Stream stream, long startOffset, long endExclusive, out string value)
    {
        if (startOffset < 0 || endExclusive < 0 || endExclusive < startOffset)
        {
            value = string.Empty;
            return false;
        }

        stream.Position = startOffset;

        Span<byte> tmp = stackalloc byte[256];
        var total = new ArrayBufferWriter<byte>(256);

        while (stream.Position < endExclusive)
        {
            var remaining = (int)Math.Min(tmp.Length, endExclusive - stream.Position);
            var read = stream.Read(tmp[..remaining]);
            if (read <= 0)
                break;

            var slice = tmp[..read];
            var terminatorIndex = slice.IndexOf((byte)0);
            if (terminatorIndex >= 0)
            {
                total.Write(slice[..terminatorIndex]);

                try
                {
                    value = Utf8Strict.GetString(total.WrittenSpan);
                    return true;
                }
                catch (DecoderFallbackException)
                {
                    value = string.Empty;
                    return false;
                }
            }

            total.Write(slice);
        }

        value = string.Empty;
        return false;
    }

    private Wdc5RowReader MoveToFieldStart(int fieldIndex, Wdc5RowReader reader)
    {
        var localReader = reader;
        var fieldBitOffset = ColumnMeta[fieldIndex].RecordOffset;

        // ColumnMeta.RecordOffset is relative to the start of the record, not the start of the section blob.
        localReader.PositionBits = reader.PositionBits + fieldBitOffset;

        return localReader;
    }

    private static T[] ReadNoneArray<T>(ref Wdc5RowReader reader, FieldMetaData fieldMeta, ColumnMetaData columnMeta) where T : unmanaged
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

    private static T[] ReadPalletArray<T>(ref Wdc5RowReader reader, ColumnMetaData columnMeta, uint[] palletData) where T : unmanaged
    {
        var cardinality = columnMeta.Pallet.Cardinality;
        var palletArrayIndex = reader.ReadUInt32(columnMeta.Pallet.BitWidth);

        var array = new T[cardinality];
        for (var i = 0; i < array.Length; i++)
        {
            var raw = palletData[i + cardinality * (int)palletArrayIndex];
            array[i] = Unsafe.As<uint, T>(ref raw);
        }

        return array;
    }

    private readonly struct DecryptedRowLease(byte[] buffer, int clearLength, int rowSizeBytes) : IDisposable
    {
        private readonly byte[]? _buffer = buffer;
        private readonly int _clearLength = clearLength;
        private readonly int _rowSizeBytes = rowSizeBytes;

        public ReadOnlySpan<byte> Bytes => _buffer.AsSpan(0, _rowSizeBytes);

        public Wdc5RowReader CreateReaderAtRowStart() => new(Bytes, positionBits: 0);

        public void Dispose()
        {
            if (_buffer is null)
                return;

            if (_clearLength > 0 && _clearLength <= _buffer.Length)
                Array.Clear(_buffer, 0, _clearLength);

            ArrayPool<byte>.Shared.Return(_buffer);
        }
    }

    private static DecryptedRowLease DecryptRowBytes(Wdc5Section section, int rowStartByte, int rowSizeBytes, int nonceId)
    {
        if (!section.IsDecryptable)
            throw new InvalidOperationException("Section is not decryptable.");

        if (rowStartByte < 0)
            throw new InvalidDataException("Row start byte is negative.");

        if (rowSizeBytes < 0)
            throw new InvalidDataException("Row size is negative.");

        if (section.RecordsData is null)
            throw new InvalidOperationException("Record bytes are not materialized for this section.");

        var endExclusive = (long)rowStartByte + rowSizeBytes;
        if (endExclusive < 0 || endExclusive > section.RecordsData.Length)
            throw new InvalidDataException("Encrypted WDC5 row points outside section record data.");

        var ciphertext = section.RecordsData.AsSpan(rowStartByte, rowSizeBytes);
        return DecryptRowBytes(ciphertext, nonceId, section.TactKey.Span);
    }

    private static DecryptedRowLease DecryptRowBytes(ReadOnlySpan<byte> ciphertext, int nonceId, ReadOnlySpan<byte> tactKey)
    {
        var rowSizeBytes = ciphertext.Length;
        if (rowSizeBytes < 0)
            throw new InvalidDataException("Row size is negative.");

        var buffer = ArrayPool<byte>.Shared.Rent(rowSizeBytes + 8);
        var dst = buffer.AsSpan(0, rowSizeBytes);
        ciphertext.CopyTo(dst);
        Array.Clear(buffer, rowSizeBytes, 8);

        Span<byte> nonce = stackalloc byte[8];
        BinaryPrimitives.WriteUInt64LittleEndian(nonce, unchecked((ulong)nonceId));

        using (var salsa = new Salsa20(tactKey, nonce))
        {
            var span = buffer.AsSpan(0, rowSizeBytes);
            salsa.Transform(span, span);
        }

        return new DecryptedRowLease(buffer, clearLength: rowSizeBytes, rowSizeBytes: rowSizeBytes);
    }

    /// <summary>
    /// Disposes the underlying stream and associated resources.
    /// </summary>
    public void Dispose()
    {
        _cachedRowValid = false;
        _cachedRowBytes = null;
        _reader.Dispose();
        _stream.Dispose();
    }
}
