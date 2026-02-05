using Shouldly;

namespace MimironSQL.Formats.Wdc5.Tests;

public sealed class Wdc5RowReaderTests
{
    [Fact]
    public void ReadUInt64_numBits_0_returns_0_and_does_not_advance_position()
    {
        var reader = new Wdc5RowReader(bytes: [0xFF], positionBits: 0);
        reader.ReadUInt64(numBits: 0).ShouldBe(0UL);
        reader.PositionBits.ShouldBe(0);
    }

    [Fact]
    public void ReadUInt64_numBits_greater_than_64_throws()
    {
        Should.Throw<ArgumentOutOfRangeException>(() =>
        {
            var reader = new Wdc5RowReader(bytes: [0x00], positionBits: 0);
            reader.ReadUInt64(numBits: 65);
        });
    }

    [Fact]
    public void ReadUInt64_reads_little_endian_using_partial_buffer_path()
    {
        // Only 3 bytes available, which forces the <8-byte path.
        var reader = new Wdc5RowReader(bytes: [0xAA, 0xBB, 0xCC], positionBits: 0);
        reader.ReadUInt64(numBits: 16).ShouldBe(0xBBAAUL);
        reader.PositionBits.ShouldBe(16);
    }

    [Fact]
    public void ReadUInt64Signed_sign_extends_negative_values()
    {
        // Read 0b111 (3 bits) which should sign-extend to -1.
        var reader = new Wdc5RowReader(bytes: [0b0000_0111], positionBits: 0);
        reader.ReadUInt64Signed(numBits: 3).ShouldBe(unchecked((ulong)-1));
        reader.PositionBits.ShouldBe(3);
    }

    [Fact]
    public void ReadCString_reads_until_null_terminator()
    {
        var reader = new Wdc5RowReader(bytes: [(byte)'h', (byte)'i', 0], positionBits: 0);
        reader.ReadCString().ShouldBe("hi");
    }
}
