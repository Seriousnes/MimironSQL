using MimironSQL.Db2;
using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Text;

namespace MimironSQL.Db2.Wdc5;

public readonly struct Wdc5Row
{
    private static readonly UTF8Encoding Utf8Strict = new(encoderShouldEmitUTF8Identifier: false, throwOnInvalidBytes: true);

    private readonly Wdc5File _file;
    private readonly Wdc5Section _section;
    private readonly BitReader _reader;

    public int GlobalRowIndex { get; }
    public int RowIndexInSection { get; }
    public int Id { get; }

    internal Wdc5Row(Wdc5File file, Wdc5Section section, BitReader reader, int globalRowIndex, int rowIndexInSection, int id)
    {
        _file = file;
        _section = section;
        _reader = reader;
        GlobalRowIndex = globalRowIndex;
        RowIndexInSection = rowIndexInSection;
        Id = id;
    }

    public T GetScalar<T>(int fieldIndex) where T : unmanaged
    {
        if ((uint)fieldIndex >= (uint)_file.Header.FieldsCount)
            throw new ArgumentOutOfRangeException(nameof(fieldIndex));

        var localReader = MoveToFieldStart(fieldIndex);
        ref readonly var fieldMeta = ref _file.FieldMeta[fieldIndex];
        ref readonly var columnMeta = ref _file.ColumnMeta[fieldIndex];
        return Wdc5FieldDecoder.ReadScalar<T>(Id, ref localReader, fieldMeta, columnMeta, _file.PalletData[fieldIndex], _file.CommonData[fieldIndex]);
    }

    public T[] GetArray<T>(int fieldIndex) where T : unmanaged
    {
        if ((uint)fieldIndex >= (uint)_file.Header.FieldsCount)
            throw new ArgumentOutOfRangeException(nameof(fieldIndex));

        var localReader = MoveToFieldStart(fieldIndex);
        ref readonly var fieldMeta = ref _file.FieldMeta[fieldIndex];
        ref readonly var columnMeta = ref _file.ColumnMeta[fieldIndex];
        var palletData = _file.PalletData[fieldIndex];
        var commonData = _file.CommonData[fieldIndex];

        return columnMeta.CompressionType switch
        {
            CompressionType.None => ReadNoneArray<T>(ref localReader, fieldMeta, columnMeta),
            CompressionType.PalletArray => ReadPalletArray<T>(ref localReader, columnMeta, palletData),
            _ => throw new NotSupportedException($"Array decode not supported for compression type {columnMeta.CompressionType} (field {fieldIndex})."),
        };
    }

    public bool TryGetDenseString(int fieldIndex, out string value)
    {
        if ((uint)fieldIndex >= (uint)_file.Header.FieldsCount)
            throw new ArgumentOutOfRangeException(nameof(fieldIndex));

        var localReader = MoveToFieldStart(fieldIndex);
        ref readonly var fieldMeta = ref _file.FieldMeta[fieldIndex];
        ref readonly var columnMeta = ref _file.ColumnMeta[fieldIndex];

        if (columnMeta.CompressionType is not (CompressionType.None or CompressionType.Immediate or CompressionType.SignedImmediate))
        {
            value = string.Empty;
            return false;
        }

        var fieldStartInBlob = (long)_section.RecordsBaseOffsetInBlob + (localReader.PositionBits >> 3);
        var offset = Wdc5FieldDecoder.ReadScalar<int>(Id, ref localReader, fieldMeta, columnMeta, _file.PalletData[fieldIndex], _file.CommonData[fieldIndex]);
        var stringAbsInBlob = fieldStartInBlob + offset;

        var stringIndex = stringAbsInBlob - _file.RecordsBlobSizeBytes;

        // In WDC2+, an offset of 0 commonly represents an empty string, which produces a negative index.
        if (stringIndex < 0)
        {
            value = string.Empty;
            return true;
        }

        if (stringIndex > int.MaxValue)
        {
            value = string.Empty;
            return false;
        }

        return TryReadNullTerminatedUtf8(_file.DenseStringTableBytes, startIndex: (int)stringIndex, endExclusive: _file.DenseStringTableBytes.Length, out value);
    }

    public bool TryGetInlineString(int fieldIndex, out string value)
    {
        if ((uint)fieldIndex >= (uint)_file.Header.FieldsCount)
            throw new ArgumentOutOfRangeException(nameof(fieldIndex));

        var localReader = MoveToFieldStart(fieldIndex);
        ref readonly var fieldMeta = ref _file.FieldMeta[fieldIndex];
        ref readonly var columnMeta = ref _file.ColumnMeta[fieldIndex];

        var rowStartInSection = _reader.PositionBits >> 3;
        var rowSizeBytes = _file.Header.Flags.HasFlag(Db2Flags.Sparse)
            ? (int)_section.SparseEntries[RowIndexInSection].Size
            : _file.Header.RecordSize;
        var rowEndExclusive = rowStartInSection + rowSizeBytes;

        // WDC5 sparse sections inline C-strings directly in record data.
        if (_file.Header.Flags.HasFlag(Db2Flags.Sparse))
        {
            var fieldStart = localReader.PositionBits >> 3;
            if ((uint)fieldStart >= (uint)rowEndExclusive)
            {
                value = string.Empty;
                return false;
            }

            if (TryReadNullTerminatedUtf8(_section.RecordsData, startIndex: fieldStart, endExclusive: rowEndExclusive, out value))
                return true;
        }

        if (columnMeta.CompressionType is not (CompressionType.None or CompressionType.Immediate or CompressionType.SignedImmediate))
        {
            value = string.Empty;
            return false;
        }

        var fieldStartInBlob = (long)_section.RecordsBaseOffsetInBlob + (localReader.PositionBits >> 3);
        var offset = Wdc5FieldDecoder.ReadScalar<int>(Id, ref localReader, fieldMeta, columnMeta, _file.PalletData[fieldIndex], _file.CommonData[fieldIndex]);
        var stringAbsInBlob = fieldStartInBlob + offset;

        if (stringAbsInBlob < 0 || stringAbsInBlob > _file.RecordsBlobSizeBytes)
        {
            value = string.Empty;
            return false;
        }

        var stringStartInSection = (long)stringAbsInBlob - _section.RecordsBaseOffsetInBlob;
        if (stringStartInSection < 0 || stringStartInSection > int.MaxValue)
        {
            value = string.Empty;
            return false;
        }

        var stringStart = (int)stringStartInSection;
        if ((uint)stringStart < (uint)rowStartInSection || (uint)stringStart >= (uint)rowEndExclusive)
        {
            value = string.Empty;
            return false;
        }

        return TryReadNullTerminatedUtf8(_section.RecordsData, startIndex: stringStart, endExclusive: rowEndExclusive, out value);
    }

    public bool TryGetString(int fieldIndex, out string value)
        => TryGetDenseString(fieldIndex, out value) || TryGetInlineString(fieldIndex, out value);

    private static bool TryReadNullTerminatedUtf8(byte[] bytes, int startIndex, int endExclusive, out string value)
    {
        if ((uint)startIndex >= (uint)bytes.Length || (uint)endExclusive > (uint)bytes.Length || startIndex >= endExclusive)
        {
            value = string.Empty;
            return false;
        }

        var span = bytes.AsSpan(startIndex, endExclusive - startIndex);
        var terminatorIndex = span.IndexOf((byte)0);
        if (terminatorIndex <= 0)
        {
            value = string.Empty;
            return false;
        }

        try
        {
            value = Utf8Strict.GetString(span[..terminatorIndex]);
            return !string.IsNullOrWhiteSpace(value);
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
}
