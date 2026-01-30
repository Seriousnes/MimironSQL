using System.Runtime.InteropServices;

namespace MimironSQL.Formats.Wdc5;

[StructLayout(LayoutKind.Explicit, Pack = 1, Size = 24)]
public struct ColumnMetaData
{
    [FieldOffset(0)]
    public ushort RecordOffset;

    [FieldOffset(2)]
    public ushort Size;

    [FieldOffset(4)]
    public uint AdditionalDataSize;

    [FieldOffset(8)]
    public CompressionType CompressionType;

    [FieldOffset(12)]
    public ColumnCompressionDataImmediate Immediate;

    [FieldOffset(12)]
    public ColumnCompressionDataPallet Pallet;

    [FieldOffset(12)]
    public ColumnCompressionDataCommon Common;
}

public readonly record struct ColumnCompressionDataImmediate(int BitOffset, int BitWidth, int Flags);

public readonly record struct ColumnCompressionDataPallet(int BitOffset, int BitWidth, int Cardinality);

public readonly record struct ColumnCompressionDataCommon(uint DefaultValue, int B, int C);
