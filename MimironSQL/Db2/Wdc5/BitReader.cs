using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Text;

namespace MimironSQL.Db2.Wdc5;

public struct BitReader(byte[] data)
{

    public int PositionBits { get; set; } = 0;
    public int OffsetBytes { get; set; } = 0;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public uint ReadUInt32(int numBits)
    {
        ref byte start = ref data[OffsetBytes + (PositionBits >> 3)];
        var result = Unsafe.As<byte, uint>(ref start) << (32 - numBits - (PositionBits & 7)) >> (32 - numBits);
        PositionBits += numBits;
        return result;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ulong ReadUInt64(int numBits)
    {
        ref byte start = ref data[OffsetBytes + (PositionBits >> 3)];
        var result = Unsafe.As<byte, ulong>(ref start) << (64 - numBits - (PositionBits & 7)) >> (64 - numBits);
        PositionBits += numBits;
        return result;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public Value32 ReadValue32(int numBits)
    {
        var raw = ReadUInt32(numBits);
        return Unsafe.As<uint, Value32>(ref raw);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
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
        return Encoding.UTF8.GetString(bytes.ToArray());
    }
}
