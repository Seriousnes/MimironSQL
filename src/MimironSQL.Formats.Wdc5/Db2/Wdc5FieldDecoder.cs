using System.Runtime.CompilerServices;

namespace MimironSQL.Formats.Wdc5.Db2;

internal static class Wdc5FieldDecoder
{
    public static T ReadScalar<T>(int id, ref Wdc5RowReader reader, FieldMetaData fieldMeta, ColumnMetaData columnMeta, uint[] palletData, Dictionary<int, uint> commonData)
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
                    return Wdc5Value.UnsafeRead<T>(raw);
                }
            case CompressionType.SignedImmediate:
                {
                    var raw = reader.ReadUInt64Signed(columnMeta.Immediate.BitWidth);
                    return Wdc5Value.UnsafeRead<T>(raw);
                }
            case CompressionType.Immediate:
                {
                    var raw = reader.ReadUInt64(columnMeta.Immediate.BitWidth);
                    return Wdc5Value.UnsafeRead<T>(raw);
                }
            case CompressionType.Common:
                {
                    if (commonData.TryGetValue(id, out var value))
                        return Wdc5Value.UnsafeRead32<T>(value);
                    return Wdc5Value.UnsafeRead32<T>(columnMeta.Common.DefaultValue);
                }
            case CompressionType.Pallet:
                {
                    var palletIndex = reader.ReadUInt32(columnMeta.Pallet.BitWidth);
                    return Wdc5Value.UnsafeRead32<T>(palletData[palletIndex]);
                }
            case CompressionType.PalletArray:
                {
                    var palletIndex = reader.ReadUInt32(columnMeta.Pallet.BitWidth);

                    return columnMeta switch
                    {
                        { Pallet.Cardinality: 1 } => Wdc5Value.UnsafeRead32<T>(palletData[palletIndex]),
                        _ => default,
                    };
                }
            default:
                throw new NotSupportedException($"Unexpected compression type {columnMeta.CompressionType}.");
        }
    }

    private static class Wdc5Value
    {
        public static T UnsafeRead<T>(ulong raw) where T : unmanaged
        {
            return Unsafe.SizeOf<T>() switch
            {
                <= 8 => Unsafe.As<ulong, T>(ref raw),
                _ => throw new NotSupportedException($"Unsupported scalar size {Unsafe.SizeOf<T>()} for {typeof(T).FullName}."),
            };
        }

        public static T UnsafeRead32<T>(uint raw) where T : unmanaged
        {
            switch (Unsafe.SizeOf<T>())
            {
                case <= 4:
                    return Unsafe.As<uint, T>(ref raw);
                case 8:
                    {
                        ulong expanded = raw;
                        return Unsafe.As<ulong, T>(ref expanded);
                    }

                default:
                    throw new NotSupportedException($"Unsupported scalar size {Unsafe.SizeOf<T>()} for {typeof(T).FullName}.");
            }
        }
    }
}
