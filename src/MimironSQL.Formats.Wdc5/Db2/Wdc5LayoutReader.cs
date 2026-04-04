using System.Text;

namespace MimironSQL.Formats.Wdc5.Db2;

internal static class Wdc5LayoutReader
{
    private const int HeaderSize = 200;
    private const uint Wdc5Magic = 0x35434457; // "WDC5"

    public static Db2FileLayout ReadLayout(Stream stream)
    {
        ArgumentNullException.ThrowIfNull(stream);
        if (!stream.CanSeek)
        {
            throw new NotSupportedException("WDC5 layout reading requires a seekable Stream.");
        }

        if (stream.Length < HeaderSize)
        {
            throw new InvalidDataException("DB2 file is too small to be valid.");
        }

        var originalPosition = stream.Position;
        try
        {
            stream.Position = 0;
            using var reader = new BinaryReader(stream, Encoding.UTF8, leaveOpen: true);

            var magic = reader.ReadUInt32();
            if (magic != Wdc5Magic)
            {
                throw new InvalidDataException("Expected WDC5.");
            }

            reader.ReadUInt32(); // schemaVersion
            reader.ReadBytes(128); // schema string

            reader.ReadInt32(); // recordsCount
            var fieldsCount = reader.ReadInt32();

            reader.ReadInt32(); // recordSize
            reader.ReadInt32(); // stringTableSize
            reader.ReadUInt32(); // tableHash
            var layoutHash = reader.ReadUInt32();

            return new Db2FileLayout(layoutHash, fieldsCount);
        }
        finally
        {
            stream.Position = originalPosition;
        }
    }
}
