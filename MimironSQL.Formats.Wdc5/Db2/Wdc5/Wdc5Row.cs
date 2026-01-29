using System.Buffers;
using System.Buffers.Binary;
using System.Runtime.CompilerServices;
using System.Text;

using MimironSQL.Db2;
using MimironSQL.Formats;

using Security.Cryptography;

namespace MimironSQL.Formats.Wdc5;

public readonly struct Wdc5Row : IDb2Row
{
    private static readonly UTF8Encoding Utf8Strict = new(encoderShouldEmitUTF8Identifier: false, throwOnInvalidBytes: true);

    private readonly Wdc5File _file;
    private readonly Wdc5Section _section;
    private readonly BitReader _reader;

    internal int GlobalRowIndex { get; }
    internal int RowIndexInSection { get; }
    internal int Id { get; }
    internal int SourceId { get; }
    internal int ParentRelationId { get; }

    internal Wdc5Row(Wdc5File file, Wdc5Section section, BitReader reader, int globalRowIndex, int rowIndexInSection, int id, int sourceId, int parentRelationId)
    {
        _file = file;
        _section = section;
        _reader = reader;
        GlobalRowIndex = globalRowIndex;
        RowIndexInSection = rowIndexInSection;
        Id = id;
        SourceId = sourceId;
        ParentRelationId = parentRelationId;
    }

    public T Get<T>(int fieldIndex)
    {
        if (fieldIndex < 0)
        {
            if (fieldIndex == Db2VirtualFieldIndex.Id)
                return ConvertVirtual<T>(Id);

            if (fieldIndex == Db2VirtualFieldIndex.ParentRelation)
                return ConvertVirtual<T>(ParentRelationId);

            throw new NotSupportedException($"Unsupported virtual field index {fieldIndex}.");
        }

        if (typeof(T) == typeof(string))
        {
            _ = TryGetString(fieldIndex, out var s);
            return (T)(object)s;
        }

        if (typeof(T) == typeof(double))
            return (T)(object)Convert.ToDouble(GetScalar<float>(fieldIndex));

        if (typeof(T).IsArray)
        {
            if (typeof(T) == typeof(byte[]))
                return (T)(object)GetArray<byte>(fieldIndex);
            if (typeof(T) == typeof(sbyte[]))
                return (T)(object)GetArray<sbyte>(fieldIndex);
            if (typeof(T) == typeof(short[]))
                return (T)(object)GetArray<short>(fieldIndex);
            if (typeof(T) == typeof(ushort[]))
                return (T)(object)GetArray<ushort>(fieldIndex);
            if (typeof(T) == typeof(int[]))
                return (T)(object)GetArray<int>(fieldIndex);
            if (typeof(T) == typeof(uint[]))
                return (T)(object)GetArray<uint>(fieldIndex);
            if (typeof(T) == typeof(long[]))
                return (T)(object)GetArray<long>(fieldIndex);
            if (typeof(T) == typeof(ulong[]))
                return (T)(object)GetArray<ulong>(fieldIndex);
            if (typeof(T) == typeof(float[]))
                return (T)(object)GetArray<float>(fieldIndex);
            if (typeof(T) == typeof(double[]))
            {
                var floats = GetArray<float>(fieldIndex);
                var doubles = new double[floats.Length];
                for (var i = 0; i < floats.Length; i++)
                    doubles[i] = floats[i];
                return (T)(object)doubles;
            }

            throw new NotSupportedException($"Unsupported array type {typeof(T).FullName}.");
        }

        var nonNullable = Nullable.GetUnderlyingType(typeof(T)) ?? typeof(T);
        if (nonNullable.IsEnum)
        {
            var underlying = Enum.GetUnderlyingType(nonNullable);
            object underlyingValue = underlying == typeof(byte)
                ? Get<byte>(fieldIndex)
                : underlying == typeof(sbyte)
                    ? Get<sbyte>(fieldIndex)
                    : underlying == typeof(short)
                        ? Get<short>(fieldIndex)
                        : underlying == typeof(ushort)
                            ? Get<ushort>(fieldIndex)
                            : underlying == typeof(int)
                                ? Get<int>(fieldIndex)
                                : underlying == typeof(uint)
                                    ? Get<uint>(fieldIndex)
                                    : underlying == typeof(long)
                                        ? Get<long>(fieldIndex)
                                        : underlying == typeof(ulong)
                                            ? Get<ulong>(fieldIndex)
                                            : throw new NotSupportedException($"Unsupported enum underlying type {underlying.FullName}.");

            var enumObj = Enum.ToObject(nonNullable, underlyingValue);
            return typeof(T) == nonNullable ? (T)enumObj : (T)Convert.ChangeType(enumObj, nonNullable);
        }

        if (typeof(T) == typeof(bool))
            return (T)(object)(GetScalar<long>(fieldIndex) != 0);

        if (typeof(T) == typeof(byte))
            return (T)(object)checked((byte)GetScalar<ulong>(fieldIndex));
        if (typeof(T) == typeof(sbyte))
            return (T)(object)checked((sbyte)GetScalar<long>(fieldIndex));
        if (typeof(T) == typeof(short))
            return (T)(object)checked((short)GetScalar<long>(fieldIndex));
        if (typeof(T) == typeof(ushort))
            return (T)(object)checked((ushort)GetScalar<ulong>(fieldIndex));
        if (typeof(T) == typeof(int))
            return (T)(object)checked((int)GetScalar<long>(fieldIndex));
        if (typeof(T) == typeof(uint))
            return (T)(object)checked((uint)GetScalar<ulong>(fieldIndex));
        if (typeof(T) == typeof(long))
            return (T)(object)GetScalar<long>(fieldIndex);
        if (typeof(T) == typeof(ulong))
            return (T)(object)GetScalar<ulong>(fieldIndex);
        if (typeof(T) == typeof(float))
            return (T)(object)GetScalar<float>(fieldIndex);

        if (Nullable.GetUnderlyingType(typeof(T)) is not null)
        {
            var underlyingType = Nullable.GetUnderlyingType(typeof(T))!;
            if (underlyingType == typeof(int))
                return (T)(object)(int?)Get<int>(fieldIndex);
            if (underlyingType == typeof(uint))
                return (T)(object)(uint?)Get<uint>(fieldIndex);
            if (underlyingType == typeof(long))
                return (T)(object)(long?)Get<long>(fieldIndex);
            if (underlyingType == typeof(ulong))
                return (T)(object)(ulong?)Get<ulong>(fieldIndex);
            if (underlyingType == typeof(short))
                return (T)(object)(short?)Get<short>(fieldIndex);
            if (underlyingType == typeof(ushort))
                return (T)(object)(ushort?)Get<ushort>(fieldIndex);
            if (underlyingType == typeof(byte))
                return (T)(object)(byte?)Get<byte>(fieldIndex);
            if (underlyingType == typeof(sbyte))
                return (T)(object)(sbyte?)Get<sbyte>(fieldIndex);
            if (underlyingType == typeof(bool))
                return (T)(object)(bool?)Get<bool>(fieldIndex);
            if (underlyingType == typeof(float))
                return (T)(object)(float?)Get<float>(fieldIndex);
            if (underlyingType == typeof(double))
                return (T)(object)(double?)Get<double>(fieldIndex);
        }

        throw new NotSupportedException($"Unsupported target type {typeof(T).FullName}.");
    }

    private static T ConvertVirtual<T>(int value)
    {
        if (typeof(T) == typeof(int))
            return (T)(object)value;
        if (typeof(T) == typeof(uint))
            return (T)(object)checked((uint)value);
        if (typeof(T) == typeof(long))
            return (T)(object)(long)value;
        if (typeof(T) == typeof(ulong))
            return (T)(object)checked((ulong)value);
        if (typeof(T) == typeof(short))
            return (T)(object)checked((short)value);
        if (typeof(T) == typeof(ushort))
            return (T)(object)checked((ushort)value);
        if (typeof(T) == typeof(byte))
            return (T)(object)checked((byte)value);
        if (typeof(T) == typeof(sbyte))
            return (T)(object)checked((sbyte)value);
        if (typeof(T) == typeof(bool))
            return (T)(object)(value != 0);
        if (typeof(T) == typeof(float))
            return (T)(object)(float)value;
        if (typeof(T) == typeof(double))
            return (T)(object)(double)value;

        if (Nullable.GetUnderlyingType(typeof(T)) is { } underlying)
        {
            if (underlying == typeof(int))
                return (T)(object)(int?)value;
            if (underlying == typeof(uint))
                return (T)(object)(uint?)checked((uint)value);
            if (underlying == typeof(long))
                return (T)(object)(long?)value;
            if (underlying == typeof(ulong))
                return (T)(object)(ulong?)checked((ulong)value);
            if (underlying == typeof(short))
                return (T)(object)(short?)checked((short)value);
            if (underlying == typeof(ushort))
                return (T)(object)(ushort?)checked((ushort)value);
            if (underlying == typeof(byte))
                return (T)(object)(byte?)checked((byte)value);
            if (underlying == typeof(sbyte))
                return (T)(object)(sbyte?)checked((sbyte)value);
            if (underlying == typeof(bool))
                return (T)(object)(bool?)(value != 0);
            if (underlying == typeof(float))
                return (T)(object)(float?)value;
            if (underlying == typeof(double))
                return (T)(object)(double?)value;
        }

        throw new NotSupportedException($"Unsupported virtual numeric conversion to {typeof(T).FullName}.");
    }

    internal bool TryGetDenseStringTableIndex(int fieldIndex, out int stringTableIndex)
    {
        if (_file.Header.Flags.HasFlag(Db2Flags.Sparse))
        {
            stringTableIndex = -1;
            return false;
        }

        if ((uint)fieldIndex >= (uint)_file.Header.FieldsCount)
            throw new ArgumentOutOfRangeException(nameof(fieldIndex));

        var fieldBitOffset = _file.ColumnMeta[fieldIndex].RecordOffset;

        DecryptedRowLease decrypted = default;
        try
        {
            var isDecrypted = _section.IsDecryptable;
            var localReader = isDecrypted
                ? (decrypted = DecryptRowBytes()).Reader
                : MoveToFieldStart(fieldIndex);

            if (isDecrypted)
                localReader.PositionBits = fieldBitOffset;

            ref readonly var fieldMeta = ref _file.FieldMeta[fieldIndex];
            ref readonly var columnMeta = ref _file.ColumnMeta[fieldIndex];

            if (columnMeta is { CompressionType: CompressionType.PalletArray, Pallet.Cardinality: not 1 })
            {
                stringTableIndex = -1;
                return false;
            }

            var offset = Wdc5FieldDecoder.ReadScalar<int>(Id, ref localReader, fieldMeta, columnMeta, _file.PalletData[fieldIndex], _file.CommonData[fieldIndex]);
            if (offset <= 0)
            {
                stringTableIndex = -1;
                return false;
            }

            var recordOffset = (long)(GlobalRowIndex * _file.Header.RecordSize) - _file.RecordsBlobSizeBytes;
            var fieldStartBytes = (long)(_file.ColumnMeta[fieldIndex].RecordOffset >> 3);
            var idx = recordOffset + fieldStartBytes + offset;

            if (idx < _section.StringTableBaseOffset)
            {
                stringTableIndex = -1;
                return false;
            }

            var sectionEndExclusive = (long)_section.StringTableBaseOffset + _section.Header.StringTableSize;
            if (idx >= sectionEndExclusive || idx is > int.MaxValue)
            {
                stringTableIndex = -1;
                return false;
            }

            stringTableIndex = (int)idx;
            return true;
        }
        finally
        {
            decrypted.Dispose();
        }
    }

    private T GetScalar<T>(int fieldIndex) where T : unmanaged
    {
        if ((uint)fieldIndex >= (uint)_file.Header.FieldsCount)
            throw new ArgumentOutOfRangeException(nameof(fieldIndex));

        Wdc5RowReadTracker.OnScalar(fieldIndex);

        if (_section.IsDecryptable)
        {
            using var decrypted = DecryptRowBytes();
            var localReader = decrypted.Reader;
            localReader.PositionBits = _file.ColumnMeta[fieldIndex].RecordOffset;
            ref readonly var fieldMeta = ref _file.FieldMeta[fieldIndex];
            ref readonly var columnMeta = ref _file.ColumnMeta[fieldIndex];
            return Wdc5FieldDecoder.ReadScalar<T>(Id, ref localReader, fieldMeta, columnMeta, _file.PalletData[fieldIndex], _file.CommonData[fieldIndex]);
        }

        var localReader2 = MoveToFieldStart(fieldIndex);
        ref readonly var fieldMeta2 = ref _file.FieldMeta[fieldIndex];
        ref readonly var columnMeta2 = ref _file.ColumnMeta[fieldIndex];
        return Wdc5FieldDecoder.ReadScalar<T>(Id, ref localReader2, fieldMeta2, columnMeta2, _file.PalletData[fieldIndex], _file.CommonData[fieldIndex]);
    }

    private T[] GetArray<T>(int fieldIndex) where T : unmanaged
    {
        if ((uint)fieldIndex >= (uint)_file.Header.FieldsCount)
            throw new ArgumentOutOfRangeException(nameof(fieldIndex));

        Wdc5RowReadTracker.OnArray(fieldIndex);

        if (_section.IsDecryptable)
        {
            using var decrypted = DecryptRowBytes();
            var localReader = decrypted.Reader;
            localReader.PositionBits = _file.ColumnMeta[fieldIndex].RecordOffset;
            ref readonly var fieldMeta = ref _file.FieldMeta[fieldIndex];
            ref readonly var columnMeta = ref _file.ColumnMeta[fieldIndex];
            var palletData = _file.PalletData[fieldIndex];

            return columnMeta.CompressionType switch
            {
                CompressionType.None => ReadNoneArray<T>(ref localReader, fieldMeta, columnMeta),
                CompressionType.PalletArray => ReadPalletArray<T>(ref localReader, columnMeta, palletData),
                _ => throw new NotSupportedException($"Array decode not supported for compression type {columnMeta.CompressionType} (field {fieldIndex})."),
            };
        }

        var localReader2 = MoveToFieldStart(fieldIndex);
        ref readonly var fieldMeta2 = ref _file.FieldMeta[fieldIndex];
        ref readonly var columnMeta2 = ref _file.ColumnMeta[fieldIndex];
        var palletData2 = _file.PalletData[fieldIndex];

        return columnMeta2.CompressionType switch
        {
            CompressionType.None => ReadNoneArray<T>(ref localReader2, fieldMeta2, columnMeta2),
            CompressionType.PalletArray => ReadPalletArray<T>(ref localReader2, columnMeta2, palletData2),
            _ => throw new NotSupportedException($"Array decode not supported for compression type {columnMeta2.CompressionType} (field {fieldIndex})."),
        };
    }

    private bool TryGetDenseString(int fieldIndex, out string value)
    {
        if (_file.Header.Flags.HasFlag(Db2Flags.Sparse))
        {
            value = string.Empty;
            return false;
        }

        if ((uint)fieldIndex >= (uint)_file.Header.FieldsCount)
            throw new ArgumentOutOfRangeException(nameof(fieldIndex));

        var fieldBitOffset = _file.ColumnMeta[fieldIndex].RecordOffset;

        DecryptedRowLease decrypted = default;
        try
        {
            var isDecrypted = _section.IsDecryptable;
            var localReader = isDecrypted
                ? (decrypted = DecryptRowBytes()).Reader
                : MoveToFieldStart(fieldIndex);

            if (isDecrypted)
                localReader.PositionBits = fieldBitOffset;

            ref readonly var fieldMeta = ref _file.FieldMeta[fieldIndex];
            ref readonly var columnMeta = ref _file.ColumnMeta[fieldIndex];

            if (columnMeta is { CompressionType: CompressionType.PalletArray, Pallet.Cardinality: not 1 })
            {
                value = string.Empty;
                return false;
            }

            var offset = Wdc5FieldDecoder.ReadScalar<int>(Id, ref localReader, fieldMeta, columnMeta, _file.PalletData[fieldIndex], _file.CommonData[fieldIndex]);
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

            var recordOffset = (long)(GlobalRowIndex * _file.Header.RecordSize) - _file.RecordsBlobSizeBytes;
            var fieldStartBytes = (long)(_file.ColumnMeta[fieldIndex].RecordOffset >> 3);
            var stringIndex = recordOffset + fieldStartBytes + offset;

            if (stringIndex < 0)
                stringIndex = 0;

            if (stringIndex < _section.StringTableBaseOffset)
            {
                value = string.Empty;
                return false;
            }

            var sectionEndExclusive = _section.StringTableBaseOffset + _section.Header.StringTableSize;
            if (stringIndex >= sectionEndExclusive || stringIndex is > int.MaxValue)
            {
                value = string.Empty;
                return false;
            }

            return TryReadNullTerminatedUtf8(_file.DenseStringTableBytes.Span, startIndex: (int)stringIndex, endExclusive: sectionEndExclusive, out value);
        }
        finally
        {
            decrypted.Dispose();
        }
    }

    private bool TryGetInlineString(int fieldIndex, out string? value)
    {
        if (!_file.Header.Flags.HasFlag(Db2Flags.Sparse))
        {
            value = string.Empty;
            return false;
        }

        if ((uint)fieldIndex >= (uint)_file.Header.FieldsCount)
            throw new ArgumentOutOfRangeException(nameof(fieldIndex));

        var rowStartInSection = _reader.OffsetBytes + (_reader.PositionBits >> 3);
        var rowSizeBytes = (int)_section.SparseEntries[RowIndexInSection].Size;

        ReadOnlySpan<byte> recordBytes = _section.RecordsData;
        var rowEndExclusive = rowStartInSection + rowSizeBytes;

        DecryptedRowLease decrypted = default;
        try
        {
            var localReader = _reader;
            localReader.PositionBits = _reader.PositionBits;

            if (_section.IsDecryptable)
            {
                decrypted = DecryptRowBytes();
                recordBytes = decrypted.Bytes.AsSpan(0, rowSizeBytes);
                rowEndExclusive = rowSizeBytes;
                rowStartInSection = 0;

                localReader = decrypted.Reader;
                localReader.PositionBits = 0;
            }

            for (var i = 0; i < fieldIndex; i++)
                SkipSparseField(ref localReader, i, recordBytes, rowEndExclusive);

            var fieldStart = localReader.OffsetBytes + (localReader.PositionBits >> 3);
            if (fieldStart >= 0 && fieldStart < rowEndExclusive && TryReadNullTerminatedUtf8(recordBytes, startIndex: fieldStart, endExclusive: rowEndExclusive, out value))
                return true;

            value = null;
            return false;
        }
        finally
        {
            decrypted.Dispose();
        }
    }

    private void SkipSparseField(ref BitReader reader, int fieldIndex, ReadOnlySpan<byte> recordBytes, int rowEndExclusive)
    {
        ref readonly var fieldMeta = ref _file.FieldMeta[fieldIndex];
        ref readonly var columnMeta = ref _file.ColumnMeta[fieldIndex];

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
            _ = Wdc5FieldDecoder.ReadScalar<long>(Id, ref reader, fieldMeta, columnMeta, _file.PalletData[fieldIndex], _file.CommonData[fieldIndex]);
        }
    }

    private bool TryGetString(int fieldIndex, out string value)
    {
        if ((uint)fieldIndex >= (uint)_file.Header.FieldsCount)
            throw new ArgumentOutOfRangeException(nameof(fieldIndex));

        Wdc5RowReadTracker.OnString(fieldIndex);
        return TryGetDenseString(fieldIndex, out value) || TryGetInlineString(fieldIndex, out value);
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
            value = Utf8Strict.GetString(span[..terminatorIndex]);
            return true;
        }
        catch (DecoderFallbackException)
        {
            value = string.Empty;
            return false;
        }
    }

    private BitReader MoveToFieldStart(int fieldIndex)
    {
        var localReader = _reader;
        var fieldBitOffset = _file.ColumnMeta[fieldIndex].RecordOffset;

        if (_file.Header.Flags.HasFlag(Db2Flags.Sparse))
            localReader.PositionBits = _reader.PositionBits + fieldBitOffset;
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

    private DecryptedRowLease DecryptRowBytes()
    {
        if (!_section.IsDecryptable)
            throw new InvalidOperationException("Row is not decryptable.");

        var rowStartInSection = _reader.OffsetBytes + (_reader.PositionBits >> 3);
        var rowSizeBytes = _file.Header.Flags.HasFlag(Db2Flags.Sparse)
            ? (int)_section.SparseEntries[RowIndexInSection].Size
            : _file.Header.RecordSize;

        if (rowSizeBytes < 0)
            throw new InvalidDataException("Row size is negative.");

        var rowEndExclusive = (long)rowStartInSection + rowSizeBytes;
        if (rowStartInSection < 0 || rowEndExclusive < 0 || rowEndExclusive > _section.RecordsData.Length)
            throw new InvalidDataException("Encrypted row points outside section record data.");

        var buffer = ArrayPool<byte>.Shared.Rent(rowSizeBytes + 8);
        var dst = buffer.AsSpan(0, rowSizeBytes);
        _section.RecordsData.AsSpan(rowStartInSection, rowSizeBytes).CopyTo(dst);
        Array.Clear(buffer, rowSizeBytes, 8);

        var nonceId = _file.Options is { EncryptedRowNonceStrategy: Wdc5EncryptedRowNonceStrategy.SourceId } ? SourceId : Id;
        Span<byte> nonce = stackalloc byte[8];
        BinaryPrimitives.WriteUInt64LittleEndian(nonce, unchecked((ulong)nonceId));

        using (var salsa = new Salsa20(_section.TactKey.Span, nonce))
        {
            var span = buffer.AsSpan(0, rowSizeBytes);
            salsa.Transform(span, span);
        }

        return new DecryptedRowLease(buffer, clearLength: rowSizeBytes);
    }
}
