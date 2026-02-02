using System.Buffers.Binary;
using System.Text;

namespace MimironSQL.Formats.Wdc5;

internal ref struct Wdc5RowReader(ReadOnlySpan<byte> bytes, int positionBits)
{
    private readonly ReadOnlySpan<byte> _bytes = bytes;

    public int PositionBits { get; set; } = positionBits;

    public ulong ReadUInt64(int numBits)
    {
        if ((uint)numBits > 64)
            throw new ArgumentOutOfRangeException(nameof(numBits));

        if (numBits == 0)
            return 0;

        // DB2 spec: read as little-endian, right-shift by (field_offset_bits & 7), then mask.
        var startByteIndex = PositionBits >> 3;
        var bitShift = PositionBits & 7;

        var raw = ReadUInt64LittleEndian(_bytes, startByteIndex);
        var value = raw >> bitShift;

        if (numBits != 64)
            value &= (1UL << numBits) - 1;

        PositionBits += numBits;
        return value;
    }

    public uint ReadUInt32(int numBits)
        => (uint)ReadUInt64(numBits);

    public ulong ReadUInt64Signed(int numBits)
    {
        var raw = ReadUInt64(numBits);
        var signedShift = 1UL << (numBits - 1);
        return (signedShift ^ raw) - signedShift;
    }

    public string ReadCString()
    {
        uint ch;
        List<byte> bytes = new(0x20);
        while ((ch = ReadUInt32(8)) != 0)
            bytes.Add((byte)ch);
        return Encoding.UTF8.GetString([.. bytes]);
    }

    private static ulong ReadUInt64LittleEndian(ReadOnlySpan<byte> buffer, int startIndex)
    {
        var remaining = buffer[startIndex..];
        if (remaining.Length >= 8)
            return BinaryPrimitives.ReadUInt64LittleEndian(remaining);

        Span<byte> tmp = stackalloc byte[8];
        remaining.CopyTo(tmp);
        return BinaryPrimitives.ReadUInt64LittleEndian(tmp);
    }
}
