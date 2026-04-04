using System.Runtime.CompilerServices;

using MimironSQL.Db2;

namespace MimironSQL.Formats.Wdc5.Db2;

/// <summary>
/// Scans a single WDC5 column for equality matches without materializing full rows.
/// </summary>
internal sealed class Wdc5RecordScanner
{
    private readonly Wdc5File _file;

    internal Wdc5RecordScanner(Wdc5File file)
    {
        ArgumentNullException.ThrowIfNull(file);
        _file = file;
    }

    // Future EF integration point: when the query pipeline detects a simple single-column
    // equality predicate, it can call this scanner instead of enumerating every row and
    // dispatching through ReadField<T>() for each candidate.
    internal void ScanFieldEquals<T>(int fieldIndex, T value, List<RowHandle> results) where T : unmanaged
    {
        ArgumentNullException.ThrowIfNull(results);

        if ((uint)fieldIndex >= (uint)_file.Header.FieldsCount)
            throw new ArgumentOutOfRangeException(nameof(fieldIndex));

        ref readonly var fieldMeta = ref _file.FieldMeta[fieldIndex];
        ref readonly var columnMeta = ref _file.ColumnMeta[fieldIndex];
        var fieldBitWidth = GetFieldBitWidth(fieldMeta, columnMeta);
        var isArrayField = IsArrayField<T>(fieldMeta, columnMeta, fieldBitWidth);

        if (columnMeta.CompressionType == CompressionType.Common)
        {
            ScanCommonField(fieldIndex, value, fieldBitWidth, results);
            return;
        }

        for (var sectionIndex = 0; sectionIndex < _file.ParsedSections.Count; sectionIndex++)
        {
            var section = _file.ParsedSections[sectionIndex];

            if (section.IsDecryptable)
            {
                ScanFallbackSection(sectionIndex, section, fieldIndex, value, isArrayField, results);
                continue;
            }

            if (isArrayField)
            {
                ScanArraySection(sectionIndex, section, fieldIndex, value, fieldMeta, columnMeta, fieldBitWidth, results);
                continue;
            }

            var expectedRaw = EncodeSearchValue(value, fieldBitWidth, columnMeta.CompressionType);

            switch (columnMeta.CompressionType)
            {
                case CompressionType.None:
                case CompressionType.Immediate:
                case CompressionType.SignedImmediate:
                    ScanScalarSection(sectionIndex, section, fieldIndex, expectedRaw, columnMeta, fieldBitWidth, results);
                    break;

                case CompressionType.Pallet:
                case CompressionType.PalletArray:
                    ScanPalletScalarSection(sectionIndex, section, fieldIndex, expectedRaw, results);
                    break;

                default:
                    ScanFallbackSection(sectionIndex, section, fieldIndex, value, isArrayField, results);
                    break;
            }
        }
    }

    private void ScanCommonField<T>(int fieldIndex, T value, int fieldBitWidth, List<RowHandle> results) where T : unmanaged
    {
        var columnMeta = _file.ColumnMeta[fieldIndex];
        var commonData = _file.CommonData[fieldIndex];
        var expectedRaw = unchecked((uint)EncodeSearchValue(value, fieldBitWidth, CompressionType.Common));

        if (expectedRaw == columnMeta.Common.DefaultValue)
        {
            for (var sectionIndex = 0; sectionIndex < _file.ParsedSections.Count; sectionIndex++)
            {
                var section = _file.ParsedSections[sectionIndex];
                for (var rowIndex = 0; rowIndex < section.NumRecords; rowIndex++)
                {
                    var rowId = _file.GetSourceIdForAccessor(section, rowIndex);
                    if (commonData.ContainsKey(rowId))
                        continue;

                    results.Add(new RowHandle(sectionIndex, rowIndex, rowId));
                }
            }

            return;
        }

        foreach (var entry in commonData)
        {
            if (entry.Value != expectedRaw)
                continue;

            if (_file.TryGetRowHandle(entry.Key, out var handle))
                results.Add(handle);
        }
    }

    private void ScanScalarSection(int sectionIndex, Wdc5Section section, int fieldIndex, ulong expectedRaw, ColumnMetaData columnMeta, int fieldBitWidth, List<RowHandle> results)
    {
        if (_file.Header.Flags.HasFlag(Db2Flags.Sparse))
        {
            var accessor = _file.CreateSparseFieldAccessor(sectionIndex, fieldIndex);
            for (var rowIndex = 0; rowIndex < section.NumRecords; rowIndex++)
            {
                if (accessor.ReadScalarRaw(rowIndex) != expectedRaw)
                    continue;

                results.Add(new RowHandle(sectionIndex, rowIndex, _file.GetSourceIdForAccessor(section, rowIndex)));
            }

            return;
        }

        var denseAccessor = _file.CreateFieldAccessor(sectionIndex, fieldIndex);
        if (IsByteAligned(columnMeta, fieldBitWidth) && fieldBitWidth >= 24 && section.RecordsData is not null)
        {
            ScanDenseByteAlignedSection(sectionIndex, section, fieldIndex, expectedRaw, columnMeta, fieldBitWidth, results);
            return;
        }

        for (var rowIndex = 0; rowIndex < section.NumRecords; rowIndex++)
        {
            if (denseAccessor.ReadScalarRaw(rowIndex) != expectedRaw)
                continue;

            results.Add(new RowHandle(sectionIndex, rowIndex, _file.GetSourceIdForAccessor(section, rowIndex)));
        }
    }

    private void ScanDenseByteAlignedSection(int sectionIndex, Wdc5Section section, int fieldIndex, ulong expectedRaw, ColumnMetaData columnMeta, int fieldBitWidth, List<RowHandle> results)
    {
        var records = section.RecordsData;
        if (records is null)
            return;

        Span<byte> needleBuffer = stackalloc byte[8];
        var byteWidth = fieldBitWidth >> 3;
        var needle = needleBuffer[..byteWidth];
        WriteLittleEndian(expectedRaw, needle);

        var columnByteOffset = columnMeta.RecordOffset >> 3;
        var recordSize = _file.Header.RecordSize;
        var searchOffset = columnByteOffset;

        while (searchOffset <= records.Length - byteWidth)
        {
            var relativeIndex = records.AsSpan(searchOffset).IndexOf(needle);
            if (relativeIndex < 0)
                break;

            var matchOffset = searchOffset + relativeIndex;
            var positionInRecord = matchOffset % recordSize;
            if (positionInRecord != columnByteOffset)
            {
                searchOffset = matchOffset + 1;
                continue;
            }

            var rowIndex = matchOffset / recordSize;
            results.Add(new RowHandle(sectionIndex, rowIndex, _file.GetSourceIdForAccessor(section, rowIndex)));
            searchOffset = matchOffset + recordSize;
        }
    }

    private void ScanPalletScalarSection(int sectionIndex, Wdc5Section section, int fieldIndex, ulong expectedRaw, List<RowHandle> results)
    {
        if (_file.Header.Flags.HasFlag(Db2Flags.Sparse))
        {
            var accessor = _file.CreateSparseFieldAccessor(sectionIndex, fieldIndex);
            for (var rowIndex = 0; rowIndex < section.NumRecords; rowIndex++)
            {
                if (accessor.ReadScalarRaw(rowIndex) != expectedRaw)
                    continue;

                results.Add(new RowHandle(sectionIndex, rowIndex, _file.GetSourceIdForAccessor(section, rowIndex)));
            }

            return;
        }

        var accessor2 = _file.CreateFieldAccessor(sectionIndex, fieldIndex);
        for (var rowIndex = 0; rowIndex < section.NumRecords; rowIndex++)
        {
            if (accessor2.ReadScalarRaw(rowIndex) != expectedRaw)
                continue;

            results.Add(new RowHandle(sectionIndex, rowIndex, _file.GetSourceIdForAccessor(section, rowIndex)));
        }
    }

    private void ScanArraySection<T>(int sectionIndex, Wdc5Section section, int fieldIndex, T value, FieldMetaData fieldMeta, ColumnMetaData columnMeta, int fieldBitWidth, List<RowHandle> results) where T : unmanaged
    {
        var expectedRaw = EncodeSearchValue(value, fieldBitWidth, columnMeta.CompressionType);

        if (section.IsDecryptable)
        {
            ScanFallbackSection(sectionIndex, section, fieldIndex, value, isArrayField: true, results);
            return;
        }

        if (_file.Header.Flags.HasFlag(Db2Flags.Sparse))
        {
            var accessor = _file.CreateSparseFieldAccessor(sectionIndex, fieldIndex);
            var offsetTable = section.SparseOffsetTable?.Value ?? throw new InvalidOperationException("Sparse offset table was not initialized.");
            var records = section.RecordsData ?? throw new InvalidOperationException("Sparse section record data is not materialized.");

            for (var rowIndex = 0; rowIndex < section.NumRecords; rowIndex++)
            {
                var reader = new Wdc5RowReader(records, positionBits: offsetTable.GetFieldBitPosition(rowIndex, fieldIndex));
                if (!ArrayReaderMatches(ref reader, fieldMeta, columnMeta, fieldBitWidth, expectedRaw, _file.PalletData[fieldIndex]))
                    continue;

                results.Add(new RowHandle(sectionIndex, rowIndex, _file.GetSourceIdForAccessor(section, rowIndex)));
            }

            return;
        }

        _file.EnsureSectionRecordsMaterialized(section);
        var denseRecords = section.RecordsData ?? throw new InvalidOperationException("Dense section record data is not materialized.");

        for (var rowIndex = 0; rowIndex < section.NumRecords; rowIndex++)
        {
            var reader = new Wdc5RowReader(denseRecords, positionBits: (rowIndex * _file.Header.RecordSize * 8) + columnMeta.RecordOffset);
            if (!ArrayReaderMatches(ref reader, fieldMeta, columnMeta, fieldBitWidth, expectedRaw, _file.PalletData[fieldIndex]))
                continue;

            results.Add(new RowHandle(sectionIndex, rowIndex, _file.GetSourceIdForAccessor(section, rowIndex)));
        }
    }

    private static bool ArrayReaderMatches(ref Wdc5RowReader reader, FieldMetaData fieldMeta, ColumnMetaData columnMeta, int fieldBitWidth, ulong expectedRaw, uint[] palletData)
    {
        switch (columnMeta.CompressionType)
        {
            case CompressionType.None:
                {
                    var elementCount = columnMeta.Size / fieldBitWidth;
                    for (var i = 0; i < elementCount; i++)
                    {
                        if (reader.ReadUInt64(fieldBitWidth) == expectedRaw)
                            return true;
                    }

                    return false;
                }

            case CompressionType.PalletArray:
                {
                    var palletArrayIndex = reader.ReadUInt32(columnMeta.Pallet.BitWidth);
                    var baseIndex = columnMeta.Pallet.Cardinality * (int)palletArrayIndex;
                    var expected32 = unchecked((uint)expectedRaw);

                    for (var i = 0; i < columnMeta.Pallet.Cardinality; i++)
                    {
                        if (palletData[baseIndex + i] == expected32)
                            return true;
                    }

                    return false;
                }

            default:
                throw new NotSupportedException($"Array scans are not supported for compression type {columnMeta.CompressionType}.");
        }
    }

    private void ScanFallbackSection<T>(int sectionIndex, Wdc5Section section, int fieldIndex, T value, bool isArrayField, List<RowHandle> results) where T : unmanaged
    {
        for (var rowIndex = 0; rowIndex < section.NumRecords; rowIndex++)
        {
            var handle = new RowHandle(sectionIndex, rowIndex, _file.GetSourceIdForAccessor(section, rowIndex));
            if (!HandleMatches(handle, fieldIndex, value, isArrayField))
                continue;

            results.Add(handle);
        }
    }

    private bool HandleMatches<T>(RowHandle handle, int fieldIndex, T value, bool isArrayField) where T : unmanaged
    {
        if (!isArrayField)
            return EqualityComparer<T>.Default.Equals(_file.ReadField<T>(handle, fieldIndex), value);

        var values = _file.ReadField<T[]>(handle, fieldIndex);
        for (var i = 0; i < values.Length; i++)
        {
            if (EqualityComparer<T>.Default.Equals(values[i], value))
                return true;
        }

        return false;
    }

    private static bool IsByteAligned(ColumnMetaData columnMeta, int fieldBitWidth)
        => (columnMeta.RecordOffset & 7) == 0 && (fieldBitWidth & 7) == 0;

    private static bool IsArrayField<T>(FieldMetaData fieldMeta, ColumnMetaData columnMeta, int fieldBitWidth) where T : unmanaged
    {
        if (columnMeta.CompressionType == CompressionType.PalletArray && columnMeta.Pallet.Cardinality != 1)
            return true;

        if (columnMeta.CompressionType != CompressionType.None)
            return false;

        return columnMeta.Size > fieldBitWidth && columnMeta.Size >= Unsafe.SizeOf<T>() * 8;
    }

    private static int GetFieldBitWidth(FieldMetaData fieldMeta, ColumnMetaData columnMeta)
    {
        return columnMeta.CompressionType switch
        {
            CompressionType.None => Math.Max(32 - fieldMeta.Bits, columnMeta.Immediate.BitWidth),
            CompressionType.Immediate or CompressionType.SignedImmediate => columnMeta.Immediate.BitWidth,
            CompressionType.Pallet or CompressionType.PalletArray => columnMeta.Pallet.BitWidth,
            CompressionType.Common => 32,
            _ => throw new NotSupportedException($"Unexpected compression type {columnMeta.CompressionType}.")
        };
    }

    private static ulong EncodeSearchValue<T>(T value, int fieldBitWidth, CompressionType compressionType) where T : unmanaged
    {
        var mutableValue = value;
        ulong raw;

        if (typeof(T).IsEnum)
        {
            raw = Unsafe.SizeOf<T>() switch
            {
                1 => Unsafe.As<T, byte>(ref mutableValue),
                2 => Unsafe.As<T, ushort>(ref mutableValue),
                4 => Unsafe.As<T, uint>(ref mutableValue),
                8 => Unsafe.As<T, ulong>(ref mutableValue),
                _ => throw new NotSupportedException($"Unsupported enum size {Unsafe.SizeOf<T>()}.")
            };
        }
        else if (typeof(T) == typeof(byte))
        {
            raw = Unsafe.As<T, byte>(ref mutableValue);
        }
        else if (typeof(T) == typeof(sbyte))
        {
            var signed = Unsafe.As<T, sbyte>(ref mutableValue);
            raw = compressionType == CompressionType.SignedImmediate
                ? unchecked((ulong)(long)signed)
                : unchecked((byte)signed);
        }
        else if (typeof(T) == typeof(short))
        {
            var signed = Unsafe.As<T, short>(ref mutableValue);
            raw = compressionType == CompressionType.SignedImmediate
                ? unchecked((ulong)(long)signed)
                : unchecked((ushort)signed);
        }
        else if (typeof(T) == typeof(ushort))
        {
            raw = Unsafe.As<T, ushort>(ref mutableValue);
        }
        else if (typeof(T) == typeof(int))
        {
            var signed = Unsafe.As<T, int>(ref mutableValue);
            raw = compressionType == CompressionType.SignedImmediate
                ? unchecked((ulong)(long)signed)
                : unchecked((uint)signed);
        }
        else if (typeof(T) == typeof(uint))
        {
            raw = Unsafe.As<T, uint>(ref mutableValue);
        }
        else if (typeof(T) == typeof(long))
        {
            raw = unchecked((ulong)Unsafe.As<T, long>(ref mutableValue));
        }
        else if (typeof(T) == typeof(ulong))
        {
            raw = Unsafe.As<T, ulong>(ref mutableValue);
        }
        else if (typeof(T) == typeof(float))
        {
            raw = BitConverter.SingleToUInt32Bits(Unsafe.As<T, float>(ref mutableValue));
        }
        else if (typeof(T) == typeof(double))
        {
            raw = BitConverter.DoubleToUInt64Bits(Unsafe.As<T, double>(ref mutableValue));
        }
        else
        {
            throw new NotSupportedException($"Unsupported scan value type {typeof(T).FullName}.");
        }

        return NormalizeEncodedValue(raw, fieldBitWidth, compressionType);
    }

    private static ulong NormalizeEncodedValue(ulong raw, int fieldBitWidth, CompressionType compressionType)
        => compressionType switch
        {
            CompressionType.SignedImmediate => raw,
            CompressionType.Common or CompressionType.Pallet or CompressionType.PalletArray => MaskBits(raw, 32),
            _ => MaskBits(raw, fieldBitWidth)
        };

    private static ulong MaskBits(ulong value, int fieldBitWidth)
    {
        if (fieldBitWidth >= 64)
            return value;

        return value & ((1UL << fieldBitWidth) - 1);
    }

    private static void WriteLittleEndian(ulong value, Span<byte> destination)
    {
        for (var i = 0; i < destination.Length; i++)
            destination[i] = (byte)(value >> (8 * i));
    }

}