using System.Buffers;
using System.Buffers.Binary;
using System.Runtime.CompilerServices;
using System.Text;

using MimironSQL.Db2;
using MimironSQL.Formats;

using Security.Cryptography;

namespace MimironSQL.Formats.Wdc5.Db2;

internal sealed class Wdc5KeyLookupRowFile : IDb2File<RowHandle>, IDb2DenseStringTableIndexProvider<RowHandle>
{
    private readonly Wdc5KeyLookupMetadata _metadata;
    private readonly Wdc5KeyLookupMetadata.Section _section;
    private readonly Stream _stream;
    private readonly Wdc5FileOptions _options;

    private readonly RowHandle _handle;
    private readonly Wdc5KeyLookupMetadata.RowResolution _resolution;

    private readonly byte[] _rowBytes;
    private readonly int _rowSizeBytes;

    public Wdc5KeyLookupRowFile(
        Wdc5KeyLookupMetadata metadata,
        Wdc5KeyLookupMetadata.RowResolution resolution,
        RowHandle handle,
        Stream stream,
        Wdc5FileOptions? options)
    {
        _metadata = metadata ?? throw new ArgumentNullException(nameof(metadata));
        _resolution = resolution;
        _handle = handle;
        _stream = stream ?? throw new ArgumentNullException(nameof(stream));
        _options = options ?? new Wdc5FileOptions();

        if (!_stream.CanSeek)
            throw new NotSupportedException("WDC5 key lookup requires a seekable Stream.");

        _section = _metadata.GetSection(resolution.SectionIndex);

        (_rowBytes, _rowSizeBytes) = ReadRowBytes();
    }

    public Wdc5Header Header => _metadata.Header;

    IDb2FileHeader IDb2File.Header => Header;

    public Type RowType => typeof(RowHandle);

    public Db2Flags Flags => Header.Flags;

    public int RecordsCount => Header.RecordsCount;

    public ReadOnlyMemory<byte> DenseStringTableBytes => ReadOnlyMemory<byte>.Empty;

    public IEnumerable<RowHandle> EnumerateRowHandles()
    {
        yield return _handle;
    }

    public IEnumerable<RowHandle> EnumerateRows() => EnumerateRowHandles();

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

        if (key == _handle.RowId)
        {
            handle = _handle;
            return true;
        }

        handle = default;
        return false;
    }

    public bool TryGetRowById<TId>(TId id, out RowHandle row) where TId : IEquatable<TId>, IComparable<TId>
        => TryGetRowHandle(id, out row);

    public bool TryGetDenseStringTableIndex(RowHandle row, int fieldIndex, out int stringTableIndex)
    {
        // This key-lookup file reads dense strings directly from the stream.
        stringTableIndex = 0;
        return false;
    }

    public T ReadField<T>(RowHandle handle, int fieldIndex)
    {
        if (handle.SectionIndex != _handle.SectionIndex || handle.RowIndexInSection != _handle.RowIndexInSection)
            throw new ArgumentException("RowHandle does not belong to this key-lookup file instance.", nameof(handle));

        var readerAtStart = new Wdc5RowReader(_rowBytes.AsSpan(0, _rowSizeBytes), positionBits: 0);

        var sourceId = _resolution.SourceId;
        var destinationId = _resolution.DestinationId;
        var nonceId = _options.EncryptedRowNonceStrategy == Wdc5EncryptedRowNonceStrategy.SourceId ? sourceId : destinationId;

        var rowBytes = _rowBytes.AsSpan(0, _rowSizeBytes);

        if (_section.IsDecryptable)
        {
            using var decrypted = DecryptRowBytes(rowBytes, nonceId);
            var decryptedReaderAtStart = decrypted.CreateReaderAtRowStart();
            return ReadFieldTyped<T>(
                decryptedReaderAtStart,
                decrypted.Bytes,
                rowEndExclusive: _rowSizeBytes,
                globalRowIndex: _resolution.GlobalRowIndex,
                sourceId: sourceId,
                destinationId: destinationId,
                parentRelationId: _resolution.ParentRelationId,
                fieldIndex: fieldIndex);
        }

        return ReadFieldTyped<T>(
            readerAtStart,
            rowBytes,
            rowEndExclusive: _rowSizeBytes,
            globalRowIndex: _resolution.GlobalRowIndex,
            sourceId: sourceId,
            destinationId: destinationId,
            parentRelationId: _resolution.ParentRelationId,
            fieldIndex: fieldIndex);
    }

    private (byte[] Buffer, int RowSizeBytes) ReadRowBytes()
    {
        if (!Header.Flags.HasFlag(Db2Flags.Sparse))
        {
            var rowSizeBytes = Header.RecordSize;
            var buffer = new byte[checked(rowSizeBytes + 8)];

            var fileOffset = checked((long)_section.Header.FileOffset + (long)_resolution.RowIndexInSection * Header.RecordSize);
            _stream.Position = fileOffset;
            ReadExactly(_stream, buffer, start: 0, count: rowSizeBytes);
            Array.Clear(buffer, rowSizeBytes, 8);
            return (buffer, rowSizeBytes);
        }

        if (_section.SparseEntries is { Length: 0 } || _section.SparseRecordStartBits is { Length: 0 })
            throw new InvalidDataException("Sparse WDC5 section missing sparse metadata.");

        var entry = _section.SparseEntries[_resolution.RowIndexInSection];
        var startBits = _section.SparseRecordStartBits[_resolution.RowIndexInSection];
        var startBytes = startBits >> 3;
        var rowSize = checked((int)entry.Size);

        var buffer2 = new byte[checked(rowSize + 8)];

        var fileOffset2 = checked((long)_section.Header.FileOffset + startBytes);
        _stream.Position = fileOffset2;
        ReadExactly(_stream, buffer2, start: 0, count: rowSize);
        Array.Clear(buffer2, rowSize, 8);

        return (buffer2, rowSize);
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

    private DecryptedRowLease DecryptRowBytes(ReadOnlySpan<byte> ciphertext, int nonceId)
    {
        if (!_section.IsDecryptable)
            throw new InvalidOperationException("Row is not decryptable.");

        var buffer = ArrayPool<byte>.Shared.Rent(_rowSizeBytes + 8);
        var dst = buffer.AsSpan(0, _rowSizeBytes);
        ciphertext.CopyTo(dst);
        Array.Clear(buffer, _rowSizeBytes, 8);

        Span<byte> nonce = stackalloc byte[8];
        BinaryPrimitives.WriteUInt64LittleEndian(nonce, unchecked((ulong)nonceId));

        using (var salsa = new Salsa20(_section.TactKey.Span, nonce))
        {
            var span = buffer.AsSpan(0, _rowSizeBytes);
            salsa.Transform(span, span);
        }

        return new DecryptedRowLease(buffer, clearLength: _rowSizeBytes, rowSizeBytes: _rowSizeBytes);
    }

    private T ReadFieldTyped<T>(
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
            return (T)ReadFieldBoxedFromPrepared(readerAtStart, recordBytes, rowEndExclusive, globalRowIndex, sourceId, destinationId, parentRelationId, Enum.GetUnderlyingType(type), fieldIndex);

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
            _ = TryGetString(globalRowIndex, readerAtStart, recordBytes, rowEndExclusive, sourceId, fieldIndex, out var s);
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
        ref readonly var fieldMeta = ref _metadata.FieldMeta[fieldIndex];
        ref readonly var columnMeta = ref _metadata.ColumnMeta[fieldIndex];
        var palletData = _metadata.PalletData[fieldIndex];
        var commonData = _metadata.CommonData[fieldIndex];

        return ReadScalarTyped<T>(sourceId, ref localReader, fieldMeta, columnMeta, palletData, commonData);
    }

    private object ReadFieldBoxedFromPrepared(
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
        if (fieldIndex < 0)
        {
            return fieldIndex switch
            {
                Db2VirtualFieldIndex.Id => Convert.ChangeType(destinationId, type),
                Db2VirtualFieldIndex.ParentRelation => Convert.ChangeType(parentRelationId, type),
                _ => throw new NotSupportedException($"Unsupported virtual field index {fieldIndex}."),
            };
        }

        if (type == typeof(string))
        {
            _ = TryGetString(globalRowIndex, readerAtStart, recordBytes, rowEndExclusive, sourceId, fieldIndex, out var s);
            return s;
        }

        if (type == typeof(int)) return ReadField<int>(_handle, fieldIndex);
        if (type == typeof(uint)) return ReadField<uint>(_handle, fieldIndex);
        if (type == typeof(short)) return ReadField<short>(_handle, fieldIndex);
        if (type == typeof(ushort)) return ReadField<ushort>(_handle, fieldIndex);
        if (type == typeof(byte)) return ReadField<byte>(_handle, fieldIndex);
        if (type == typeof(sbyte)) return ReadField<sbyte>(_handle, fieldIndex);
        if (type == typeof(long)) return ReadField<long>(_handle, fieldIndex);
        if (type == typeof(ulong)) return ReadField<ulong>(_handle, fieldIndex);
        if (type == typeof(float)) return ReadField<float>(_handle, fieldIndex);
        if (type == typeof(double)) return ReadField<double>(_handle, fieldIndex);

        throw new NotSupportedException($"Unsupported field type {type.FullName}.");
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

    private Wdc5RowReader MoveToFieldStart(int fieldIndex, Wdc5RowReader reader)
    {
        if ((uint)fieldIndex >= (uint)Header.FieldsCount)
            throw new ArgumentOutOfRangeException(nameof(fieldIndex));

        if (Header.Flags.HasFlag(Db2Flags.Sparse))
            return reader;

        var fieldBitOffset = _metadata.ColumnMeta[fieldIndex].RecordOffset;
        var localReader = reader;
        localReader.PositionBits = reader.PositionBits + fieldBitOffset;
        return localReader;
    }

    private bool TryGetString(int globalRowIndex, Wdc5RowReader readerAtStart, ReadOnlySpan<byte> recordBytes, int rowEndExclusive, int id, int fieldIndex, out string value)
    {
        if ((uint)fieldIndex >= (uint)Header.FieldsCount)
            throw new ArgumentOutOfRangeException(nameof(fieldIndex));

        return TryGetDenseString(globalRowIndex, readerAtStart, id, fieldIndex, out value) ||
               TryGetInlineString(readerAtStart, recordBytes, rowEndExclusive, id, fieldIndex, out value);
    }

    private bool TryGetDenseString(int globalRowIndex, Wdc5RowReader readerAtStart, int id, int fieldIndex, out string value)
    {
        if (Header.Flags.HasFlag(Db2Flags.Sparse))
        {
            value = string.Empty;
            return false;
        }

        var localReader2 = MoveToFieldStart(fieldIndex, readerAtStart);
        ref readonly var fieldMeta2 = ref _metadata.FieldMeta[fieldIndex];
        ref readonly var columnMeta2 = ref _metadata.ColumnMeta[fieldIndex];

        if (columnMeta2 is { CompressionType: CompressionType.PalletArray, Pallet.Cardinality: not 1 })
        {
            value = string.Empty;
            return false;
        }

        var offset = Wdc5FieldDecoder.ReadScalar<int>(id, ref localReader2, fieldMeta2, columnMeta2, _metadata.PalletData[fieldIndex], _metadata.CommonData[fieldIndex]);
        switch (offset)
        {
            case 0:
                value = string.Empty;
                return true;
            case < 0:
                value = string.Empty;
                return false;
        }

        var recordOffset = (long)(globalRowIndex * Header.RecordSize) - _metadata.RecordsBlobSizeBytes;
        var fieldStartBytes = (long)(_metadata.ColumnMeta[fieldIndex].RecordOffset >> 3);
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

        var fileOffset = checked((long)_section.Header.FileOffset + _section.RecordDataSizeBytes + (stringIndex - _section.StringTableBaseOffset));
        var sectionStringsEnd = checked((long)_section.Header.FileOffset + _section.RecordDataSizeBytes + _section.Header.StringTableSize);

        return TryReadNullTerminatedUtf8FromStream(_stream, fileOffset, sectionStringsEnd, out value);
    }

    private bool TryGetInlineString(Wdc5RowReader readerAtStart, ReadOnlySpan<byte> recordBytes, int rowEndExclusive, int id, int fieldIndex, out string value)
    {
        if (!Header.Flags.HasFlag(Db2Flags.Sparse))
        {
            value = string.Empty;
            return false;
        }

        var localReader2 = readerAtStart;
        for (var i = 0; i < fieldIndex; i++)
            SkipSparseField(ref localReader2, i, recordBytes, rowEndExclusive, id);

        var fieldStart2 = localReader2.PositionBits >> 3;
        if (fieldStart2 >= 0 && fieldStart2 < rowEndExclusive && TryReadNullTerminatedUtf8(recordBytes, startIndex: fieldStart2, endExclusive: rowEndExclusive, out value))
            return true;

        value = string.Empty;
        return false;
    }

    private void SkipSparseField(ref Wdc5RowReader reader, int fieldIndex, ReadOnlySpan<byte> recordBytes, int endExclusive, int id)
    {
        ref readonly var fieldMeta = ref _metadata.FieldMeta[fieldIndex];
        ref readonly var columnMeta = ref _metadata.ColumnMeta[fieldIndex];

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
                            break;
                        }

                        reader.PositionBits = endExclusive * 8;
                        break;
                    }

                    reader.PositionBits += bitSize;
                    break;
                }

            default:
                {
                    var localReader = reader;
                    _ = Wdc5FieldDecoder.ReadScalar<uint>(id, ref localReader, fieldMeta, columnMeta, _metadata.PalletData[fieldIndex], _metadata.CommonData[fieldIndex]);
                    reader = localReader;
                    break;
                }
        }
    }

    private static bool TryReadNullTerminatedUtf8(ReadOnlySpan<byte> bytes, int startIndex, int endExclusive, out string value)
    {
        if ((uint)startIndex >= (uint)bytes.Length || startIndex >= endExclusive)
        {
            value = string.Empty;
            return false;
        }

        if ((uint)endExclusive > (uint)bytes.Length)
            endExclusive = bytes.Length;

        var slice = bytes.Slice(startIndex, endExclusive - startIndex);
        var terminator = slice.IndexOf((byte)0);
        if (terminator < 0)
        {
            value = string.Empty;
            return false;
        }

        value = Encoding.UTF8.GetString(slice.Slice(0, terminator));
        return true;
    }

    private static bool TryReadNullTerminatedUtf8FromStream(Stream stream, long startOffset, long endExclusive, out string value)
    {
        if (startOffset < 0 || endExclusive < 0 || endExclusive < startOffset)
        {
            value = string.Empty;
            return false;
        }

        stream.Position = startOffset;

        // Read in small chunks; dense strings are typically short.
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
                value = Encoding.UTF8.GetString(total.WrittenSpan);
                return true;
            }

            total.Write(slice);
        }

        value = string.Empty;
        return false;
    }

    private T[] ReadArray<T>(Wdc5RowReader readerAtStart, int fieldIndex) where T : unmanaged
    {
        if ((uint)fieldIndex >= (uint)Header.FieldsCount)
            throw new ArgumentOutOfRangeException(nameof(fieldIndex));

        var localReader2 = MoveToFieldStart(fieldIndex, readerAtStart);
        ref readonly var fieldMeta2 = ref _metadata.FieldMeta[fieldIndex];
        ref readonly var columnMeta2 = ref _metadata.ColumnMeta[fieldIndex];
        var palletData2 = _metadata.PalletData[fieldIndex];

        return columnMeta2.CompressionType switch
        {
            CompressionType.None => ReadNoneArray<T>(ref localReader2, fieldMeta2, columnMeta2),
            CompressionType.PalletArray => ReadPalletArray<T>(ref localReader2, columnMeta2, palletData2),
            _ => throw new NotSupportedException($"Array decode not supported for compression type {columnMeta2.CompressionType} (field {fieldIndex})."),
        };
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

    private object GetArrayBoxed(Wdc5RowReader readerAtStart, Type elementType, int fieldIndex)
    {
        if (elementType == typeof(float))
            return ReadArray<float>(readerAtStart, fieldIndex);
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

        throw new NotSupportedException($"Unsupported array element type {elementType.FullName}.");
    }
}
