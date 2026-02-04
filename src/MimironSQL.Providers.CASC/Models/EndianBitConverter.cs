using System.Buffers.Binary;

namespace MimironSQL.Providers;

internal static class EndianBitConverter
{
    public static ulong ReadUIntBigEndian(ReadOnlySpan<byte> bytes)
    {
        if (bytes.Length is < 1 or > 8)
            throw new ArgumentOutOfRangeException(nameof(bytes));

        ulong v = 0;
        foreach (var b in bytes)
        {
            v = (v << 8) | b;
        }
        return v;
    }

    public static uint ReadUInt32LittleEndian(ReadOnlySpan<byte> bytes) => BinaryPrimitives.ReadUInt32LittleEndian(bytes);
    public static ushort ReadUInt16LittleEndian(ReadOnlySpan<byte> bytes) => BinaryPrimitives.ReadUInt16LittleEndian(bytes);
    public static ulong ReadUInt64LittleEndian(ReadOnlySpan<byte> bytes) => BinaryPrimitives.ReadUInt64LittleEndian(bytes);
}
