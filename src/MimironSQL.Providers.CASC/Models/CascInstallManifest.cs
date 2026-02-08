using System.Buffers.Binary;
using System.Text;

namespace MimironSQL.Providers;

public sealed record CascInstallFileEntry(string Name, CascKey ContentKey, uint Size);

public static class CascInstallManifest
{
    public static IReadOnlyList<CascInstallFileEntry> Parse(ReadOnlySpan<byte> decodedInstallFile)
    {
        if (decodedInstallFile.Length < 10)
            throw new InvalidDataException("INSTALL file too small");

        if (decodedInstallFile[0] != (byte)'I' || decodedInstallFile[1] != (byte)'N')
            throw new InvalidDataException("INSTALL signature not found");

        byte version = decodedInstallFile[2];
        if (version != 1)
            throw new InvalidDataException($"Unsupported INSTALL version: {version}");

        int hashSize = decodedInstallFile[3];
        if (hashSize != CascKey.Length)
            throw new InvalidDataException($"Unsupported INSTALL hash size: {hashSize}");

        int numTags = BinaryPrimitives.ReadUInt16BigEndian(decodedInstallFile[4..6]);
        int numEntries = checked((int)BinaryPrimitives.ReadUInt32BigEndian(decodedInstallFile[6..10]));

        int offset = 10;

        int tagMaskBytes = (numEntries + 7) / 8;
        for (int i = 0; i < numTags; i++)
        {
            ReadNullTerminatedUtf8(decodedInstallFile, ref offset);
            offset += 2; // type
            offset += tagMaskBytes;

            if (offset > decodedInstallFile.Length)
                throw new InvalidDataException("INSTALL tags exceed file size");
        }

        var entries = new List<CascInstallFileEntry>(numEntries);
        for (int i = 0; i < numEntries; i++)
        {
            var name = ReadNullTerminatedUtf8(decodedInstallFile, ref offset);

            if (offset + hashSize + 4 > decodedInstallFile.Length)
                throw new InvalidDataException("INSTALL entry truncated");

            var hash = new CascKey(decodedInstallFile.Slice(offset, hashSize));
            offset += hashSize;

            uint size = BinaryPrimitives.ReadUInt32BigEndian(decodedInstallFile.Slice(offset, 4));
            offset += 4;

            entries.Add(new CascInstallFileEntry(name, hash, size));
        }

        return entries;
    }

    private static string ReadNullTerminatedUtf8(ReadOnlySpan<byte> bytes, ref int offset)
    {
        int start = offset;
        while (offset < bytes.Length && bytes[offset] != 0)
            offset++;

        if (offset >= bytes.Length)
            throw new InvalidDataException("Missing null terminator in INSTALL file");        
        var s = Encoding.UTF8.GetString(bytes[start..offset]);
        offset++; // consume '\0'
        return s;
    }
}
