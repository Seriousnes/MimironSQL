using System.Buffers.Binary;

namespace MimironSQL.Providers;

public sealed class CascShmemFile
{
    public required string DataPath { get; init; }
    public required IReadOnlyList<uint> IdxVersions { get; init; }

    public static CascShmemFile Read(Stream stream)
    {
        ArgumentNullException.ThrowIfNull(stream);
        if (!stream.CanRead) throw new ArgumentException("Stream must be readable", nameof(stream));

        // First block header (264 bytes): uint32 blockType, uint32 nextBlock, char[0x100] dataPath
        Span<byte> header = stackalloc byte[0x108];
        ReadExactly(stream, header);

        var blockType = BinaryPrimitives.ReadUInt32LittleEndian(header[0..4]);
        var nextBlock = BinaryPrimitives.ReadUInt32LittleEndian(header[4..8]);

        if (blockType is not (4u or 5u))
            throw new InvalidDataException($"Unexpected shmem block type: {blockType}");

        var dataPathRaw = header[8..(8 + 0x100)];
        int zero = dataPathRaw.IndexOf((byte)0);
        if (zero < 0) zero = dataPathRaw.Length;
        var dataPath = System.Text.Encoding.UTF8.GetString(dataPathRaw[..zero]);

        // The remainder until nextBlock contains:
        // - free space entries (8 bytes each) then
        // - idx versions (4 bytes each) (count usually 16)
        // We only reliably parse idx versions by inferring idxFileCount from folder contents later.
        // For now, parse from the end as: last N * 4 bytes, where N is 16 if it fits.

        var remaining = (int)nextBlock - header.Length;
        if (remaining < 0)
            throw new InvalidDataException("Invalid NextBlock in shmem.");

        byte[] rest = new byte[remaining];
        ReadExactly(stream, rest);

        var idxVersions = new List<uint>();
        // Heuristic: if there are at least 16*4 bytes, take the last 16 u32s as versions.
        const int typicalIdxCount = 16;
        if (rest.Length >= typicalIdxCount * 4)
        {
            int start = rest.Length - typicalIdxCount * 4;
            for (int i = 0; i < typicalIdxCount; i++)
            {
                var v = BinaryPrimitives.ReadUInt32LittleEndian(rest.AsSpan(start + i * 4, 4));
                idxVersions.Add(v);
            }
        }

        return new CascShmemFile
        {
            DataPath = dataPath,
            IdxVersions = idxVersions,
        };
    }

    private static void ReadExactly(Stream stream, Span<byte> buffer)
    {
        int readTotal = 0;
        while (readTotal < buffer.Length)
        {
            int read = stream.Read(buffer[readTotal..]);
            if (read <= 0) throw new EndOfStreamException();
            readTotal += read;
        }
    }

    private static void ReadExactly(Stream stream, byte[] buffer)
    {
        int readTotal = 0;
        while (readTotal < buffer.Length)
        {
            int read = stream.Read(buffer, readTotal, buffer.Length - readTotal);
            if (read <= 0) throw new EndOfStreamException();
            readTotal += read;
        }
    }
}
