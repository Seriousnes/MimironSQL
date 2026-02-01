using System.Buffers.Binary;
using System.Text;

namespace MimironSQL.Db2;

public static class Db2FormatDetector
{
    public static Db2Format Detect(ReadOnlySpan<byte> headerBytes)
    {
        if (headerBytes is { Length: < 4 })
            return Db2Format.Unknown;

        var magic = BinaryPrimitives.ReadUInt32LittleEndian(headerBytes);
        return magic switch
        {
            0x33434457 => Db2Format.Wdc3, // "WDC3"
            0x34434457 => Db2Format.Wdc4, // "WDC4"
            0x35434457 => Db2Format.Wdc5, // "WDC5"
            _ => Db2Format.Unknown,
        };
    }

    public static Db2Format DetectOrThrow(ReadOnlySpan<byte> headerBytes)
    {
        var format = Detect(headerBytes);
        if (format != Db2Format.Unknown)
            return format;

        var magicText = headerBytes is { Length: >= 4 } ? Encoding.ASCII.GetString(headerBytes[..4]) : "";
        throw new InvalidDataException($"Unrecognized DB2 format magic '{magicText}'.");
    }
}
