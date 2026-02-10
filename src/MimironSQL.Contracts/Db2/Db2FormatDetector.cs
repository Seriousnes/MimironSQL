using System.Buffers.Binary;
using System.Text;

namespace MimironSQL.Db2;

/// <summary>
/// Detects the DB2 binary format from header bytes.
/// </summary>
public static class Db2FormatDetector
{
    /// <summary>
    /// Detects the DB2 format from the header magic.
    /// </summary>
    /// <param name="headerBytes">A span containing at least the first 4 bytes of the file.</param>
    /// <returns>The detected DB2 format, or <see cref="Db2Format.Unknown"/> if not recognized.</returns>
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

    /// <summary>
    /// Detects the DB2 format from the header magic, throwing if not recognized.
    /// </summary>
    /// <param name="headerBytes">A span containing at least the first 4 bytes of the file.</param>
    /// <returns>The detected DB2 format.</returns>
    /// <exception cref="InvalidDataException">Thrown when the header magic is not recognized.</exception>
    public static Db2Format DetectOrThrow(ReadOnlySpan<byte> headerBytes)
    {
        var format = Detect(headerBytes);
        if (format != Db2Format.Unknown)
            return format;

        var magicText = headerBytes is { Length: >= 4 } ? Encoding.ASCII.GetString([.. headerBytes.Slice(0, 4)]) : string.Empty;
        throw new InvalidDataException($"Unrecognized DB2 format magic '{magicText}'.");
    }
}
