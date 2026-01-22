using MimironSQL.Db2;
using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;

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

        var localReader = _reader;

        // Sequential decode to match the file layout expectations.
        for (var i = 0; i <= fieldIndex; i++)
        {
            ref readonly var fieldMeta = ref _file.FieldMeta[i];
            ref readonly var columnMeta = ref _file.ColumnMeta[i];
            var palletData = _file.PalletData[i];
            var commonData = _file.CommonData[i];

            if (i == fieldIndex)
                return ReadFieldValue<T>(Id, ref localReader, fieldMeta, columnMeta, palletData, commonData);

            _ = ReadFieldValue<uint>(Id, ref localReader, fieldMeta, columnMeta, palletData, commonData);
        }

        throw new InvalidOperationException("Unreachable.");
    }

    private static T ReadFieldValue<T>(int id, ref BitReader reader, FieldMetaData fieldMeta, ColumnMetaData columnMeta, Value32[] palletData, Dictionary<int, Value32> commonData)
        where T : unmanaged
    {
        switch (columnMeta.CompressionType)
        {
            case CompressionType.None:
                {
                    var bitSize = 32 - fieldMeta.Bits;
                    if (bitSize <= 0)
                        bitSize = columnMeta.Immediate.BitWidth;

                    var raw = reader.ReadUInt64(bitSize);
                    return Unsafe.As<ulong, T>(ref raw);
                }
            case CompressionType.SignedImmediate:
                {
                    var raw = reader.ReadUInt64Signed(columnMeta.Immediate.BitWidth);
                    return Unsafe.As<ulong, T>(ref raw);
                }
            case CompressionType.Immediate:
                {
                    var raw = reader.ReadUInt64(columnMeta.Immediate.BitWidth);
                    return Unsafe.As<ulong, T>(ref raw);
                }
            case CompressionType.Common:
                {
                    if (commonData.TryGetValue(id, out var value))
                        return value.GetValue<T>();
                    return columnMeta.Common.DefaultValue.GetValue<T>();
                }
            case CompressionType.Pallet:
                {
                    var palletIndex = reader.ReadUInt32(columnMeta.Pallet.BitWidth);
                    return palletData[palletIndex].GetValue<T>();
                }
            case CompressionType.PalletArray:
                {
                    // Always consume the index bits so sequential decoding stays aligned.
                    var palletIndex = reader.ReadUInt32(columnMeta.Pallet.BitWidth);

                    if (columnMeta.Pallet.Cardinality == 1)
                        return palletData[palletIndex].GetValue<T>();

                    // Non-scalar pallet arrays are surfaced later (Phase 2 schema mapping).
                    return default;
                }
            default:
                throw new NotSupportedException($"Unexpected compression type {columnMeta.CompressionType}.");
        }
    }
}
