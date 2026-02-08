using System.Buffers.Binary;

using MimironSQL.Db2;
using MimironSQL.Formats;

using Shouldly;

namespace MimironSQL.Contracts.Tests;

public sealed class ContractsTests
{
    [Fact]
    public void Db2FormatDetector_Detect_ShortHeader_ReturnsUnknown()
    {
        Db2FormatDetector.Detect([]).ShouldBe(Db2Format.Unknown);
        Db2FormatDetector.Detect([1, 2, 3]).ShouldBe(Db2Format.Unknown);
    }

    [Fact]
    public void Db2FormatDetector_Detect_Wdc5Magic_ReturnsWdc5()
    {
        Span<byte> bytes = stackalloc byte[4];
        BinaryPrimitives.WriteUInt32LittleEndian(bytes, 0x35434457);
        Db2FormatDetector.Detect(bytes).ShouldBe(Db2Format.Wdc5);
    }

    [Fact]
    public void Db2FormatDetector_Detect_Wdc3Magic_ReturnsWdc3()
    {
        Span<byte> bytes = stackalloc byte[4];
        BinaryPrimitives.WriteUInt32LittleEndian(bytes, 0x33434457);
        Db2FormatDetector.Detect(bytes).ShouldBe(Db2Format.Wdc3);
    }

    [Fact]
    public void Db2FormatDetector_Detect_Wdc4Magic_ReturnsWdc4()
    {
        Span<byte> bytes = stackalloc byte[4];
        BinaryPrimitives.WriteUInt32LittleEndian(bytes, 0x34434457);
        Db2FormatDetector.Detect(bytes).ShouldBe(Db2Format.Wdc4);
    }

    [Fact]
    public void Db2FormatDetector_DetectOrThrow_Wdc5Magic_ReturnsWdc5()
    {
        Span<byte> bytes = stackalloc byte[4];
        BinaryPrimitives.WriteUInt32LittleEndian(bytes, 0x35434457);
        Db2FormatDetector.DetectOrThrow(bytes).ShouldBe(Db2Format.Wdc5);
    }

    [Fact]
    public void Db2FormatDetector_DetectOrThrow_Unknown_ThrowsWithMagicText()
    {
        var ex = Should.Throw<InvalidDataException>(() => Db2FormatDetector.DetectOrThrow([(byte)'N', (byte)'O', (byte)'P', (byte)'E']));
        ex.Message.ShouldContain("NOPE");
    }

    [Fact]
    public void Db2FormatDetector_DetectOrThrow_ShortHeader_ThrowsWithEmptyMagicText()
    {
        var ex = Should.Throw<InvalidDataException>(() => Db2FormatDetector.DetectOrThrow([(byte)'N', (byte)'O', (byte)'P']));
        ex.Message.ShouldContain("magic ''");
    }

    [Fact]
    public void Db2FormatRegistry_Register_Null_Throws()
    {
        var registry = new Db2FormatRegistry();
        Should.Throw<ArgumentNullException>(() => registry.Register(null!));
    }

    [Fact]
    public void Db2FormatRegistry_Register_Valid_ExposesInFormats()
    {
        var registry = new Db2FormatRegistry();
        registry.Register(new FakeFormat());
        registry.Formats.Count.ShouldBe(1);
        registry.Formats[0].Format.ShouldBe(Db2Format.Unknown);
    }

    [Fact]
    public void RowHandle_Properties_RoundTrip()
    {
        var h = new RowHandle(sectionIndex: 2, rowIndexInSection: 7, rowId: 123);
        h.SectionIndex.ShouldBe(2);
        h.RowIndexInSection.ShouldBe(7);
        h.RowId.ShouldBe(123);
        h.Handle.ShouldBe(h);
    }

    private sealed class FakeFormat : IDb2Format
    {
        public Db2Format Format => Db2Format.Unknown;
        public IDb2File OpenFile(Stream stream) => throw new NotSupportedException();
        public Db2FileLayout GetLayout(IDb2File file) => throw new NotSupportedException();
    }
}
