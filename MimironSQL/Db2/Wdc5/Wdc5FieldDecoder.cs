using System.Runtime.CompilerServices;

namespace MimironSQL.Db2.Wdc5;

internal static class Wdc5FieldDecoder
{
    public static T ReadScalar<T>(int id, ref BitReader reader, FieldMetaData fieldMeta, ColumnMetaData columnMeta, Value32[] palletData, Dictionary<int, Value32> commonData)
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
                    var palletIndex = reader.ReadUInt32(columnMeta.Pallet.BitWidth);

                    if (columnMeta.Pallet.Cardinality == 1)
                        return palletData[palletIndex].GetValue<T>();

                    return default;
                }
            default:
                throw new NotSupportedException($"Unexpected compression type {columnMeta.CompressionType}.");
        }
    }
}
