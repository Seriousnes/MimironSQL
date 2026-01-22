using MimironSQL.Db2;
using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Text;

namespace MimironSQL.Db2.Wdc5;

public readonly struct Wdc5Row
{
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
        if (_file.Header.Flags.HasFlag(Db2Flags.Sparse))
        {
            value = string.Empty;
            return false;
        }

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

        var fieldBytePos = localReader.PositionBits >> 3;
        var offset = Wdc5FieldDecoder.ReadScalar<int>(Id, ref localReader, fieldMeta, columnMeta, _file.PalletData[fieldIndex], _file.CommonData[fieldIndex]);

        var recordOffset = (GlobalRowIndex * _file.Header.RecordSize) - (_file.Header.RecordsCount * _file.Header.RecordSize);
        var stringIndex = recordOffset + fieldBytePos + offset;

        if ((uint)stringIndex >= (uint)_file.DenseStringTableBytes.Length)
        {
            value = string.Empty;
            return false;
        }

        var span = _file.DenseStringTableBytes.AsSpan(stringIndex);
        var terminatorIndex = span.IndexOf((byte)0);
        if (terminatorIndex < 0)
        {
            value = string.Empty;
            return false;
        }

        value = Encoding.UTF8.GetString(span[..terminatorIndex]);
        return true;
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
