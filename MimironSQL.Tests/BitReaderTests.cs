using MimironSQL.Db2.Wdc5;

using Shouldly;

namespace MimironSQL.Tests;

public sealed class BitReaderTests
{
    [Fact]
    public void ReadUInt32_reads_across_byte_boundary()
    {
        // The DB2 bit reader consumes bits LSB-first within a byte.
        // 0xAB => low nibble 0xB first, then high nibble 0xA.
        byte[] data = [0xAB, 0xCD, 0, 0, 0, 0, 0, 0];
        var reader = new BitReader(data);

        reader.ReadUInt32(4).ShouldBe(0xBu);
        reader.ReadUInt32(4).ShouldBe(0xAu);
        reader.ReadUInt32(4).ShouldBe(0xDu);
    }

    [Fact]
    public void ReadUInt64Signed_sign_extends()
    {
        // Read 4 bits: 0b1000 (0x8) should sign-extend to -8.
        byte[] data = [0x08, 0, 0, 0, 0, 0, 0, 0];
        var reader = new BitReader(data);

        reader.ReadUInt64Signed(4).ShouldBe(unchecked((ulong)(-8L)));
    }
}
