using System.Text;

using MimironSQL.Formats;
using MimironSQL.Formats.Wdc5;

using Shouldly;

namespace MimironSQL.Formats.Wdc5.Tests;

public sealed class Wdc5Tests
{
    [Fact]
    public void Wdc5File_Ctor_MinimalHeader_ParsesWithZeroFieldsAndSections()
    {
        using var stream = CreateMinimalWdc5Stream(layoutHash: 0x12345678, fieldsCount: 0, recordsCount: 0, sectionsCount: 0);
        var file = new Wdc5File(stream);
        Assert.Equal(0x12345678u, file.Header.LayoutHash);
        Assert.Equal(0, file.Header.FieldsCount);
        Assert.Equal(0, file.Sections.Count);
        Assert.Equal(0, file.ParsedSections.Count);
        Assert.Equal(0, file.RecordsCount);
    }

    [Fact]
    public void Wdc5Format_GetLayout_UsesHeaderHashAndFieldsCount()
    {
        using var stream = CreateMinimalWdc5Stream(layoutHash: 0xCAFEBABE, fieldsCount: 7, recordsCount: 0, sectionsCount: 0);
        var format = Wdc5Format.Instance;
        var file = (Wdc5File)format.OpenFile(stream);
        var layout = format.GetLayout(file);
        Assert.Equal(0xCAFEBABEu, layout.LayoutHash);
        Assert.Equal(7, layout.PhysicalFieldsCount);
    }

    [Fact]
    public void Wdc5File_Ctor_NonSeekableStream_Throws()
    {
        using var stream = new NonSeekableStream(new MemoryStream([1, 2, 3]));
        Should.Throw<NotSupportedException>(() => new Wdc5File(stream));
    }

    [Fact]
    public void Wdc5File_Ctor_WrongMagic_ThrowsWithDetectedFormat()
    {
        using var stream = CreateMinimalWdc5Stream(layoutHash: 0, fieldsCount: 0, recordsCount: 0, sectionsCount: 0, magic: 0x33434457u);
        var ex = Should.Throw<InvalidDataException>(() => new Wdc5File(stream));
        Assert.Contains("Wdc3", ex.Message);
    }

    private static MemoryStream CreateMinimalWdc5Stream(uint layoutHash, int fieldsCount, int recordsCount, int sectionsCount, uint magic = 0x35434457)
    {
        var ms = new MemoryStream();
        using (var writer = new BinaryWriter(ms, Encoding.UTF8, leaveOpen: true))
        {
            writer.Write(magic);
            writer.Write(1u); // schemaVersion
            writer.Write(new byte[128]); // schemaString
            writer.Write(recordsCount);
            writer.Write(fieldsCount);
            writer.Write(0); // recordSize
            writer.Write(0); // stringTableSize
            writer.Write(0u); // tableHash
            writer.Write(layoutHash);
            writer.Write(0); // minIndex
            writer.Write(0); // maxIndex
            writer.Write(0); // locale
            writer.Write((ushort)0); // flags
            writer.Write((ushort)0); // idFieldIndex
            writer.Write(0); // totalFieldsCount
            writer.Write(0); // packedDataOffset
            writer.Write(0); // lookupColumnCount
            writer.Write(0); // columnMetaDataSize
            writer.Write(0); // commonDataSize
            writer.Write(0); // palletDataSize
            writer.Write(sectionsCount);

            // Section headers (none in our minimal streams).

            // Field meta (2x Int16 per field)
            for (var i = 0; i < fieldsCount; i++)
            {
                writer.Write((short)0);
                writer.Write((short)0);
            }

            // Column meta (24 bytes per field)
            if (fieldsCount != 0)
                writer.Write(new byte[fieldsCount * 24]);

            // Ensure the stream is at least the code's minimum header threshold.
            while (ms.Length < 204)
                writer.Write((byte)0);
        }

        ms.Position = 0;
        return ms;
    }

    private sealed class NonSeekableStream(Stream inner) : Stream
    {
        public override bool CanRead => inner.CanRead;
        public override bool CanSeek => false;
        public override bool CanWrite => inner.CanWrite;
        public override long Length => inner.Length;
        public override long Position { get => inner.Position; set => throw new NotSupportedException(); }
        public override void Flush() => inner.Flush();
        public override int Read(byte[] buffer, int offset, int count) => inner.Read(buffer, offset, count);
        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
        public override void SetLength(long value) => inner.SetLength(value);
        public override void Write(byte[] buffer, int offset, int count) => inner.Write(buffer, offset, count);
        protected override void Dispose(bool disposing)
        {
            if (disposing)
                inner.Dispose();
            base.Dispose(disposing);
        }
    }
}
