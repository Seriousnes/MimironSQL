using System.Buffers.Binary;

namespace MimironSQL.Providers;

public sealed class CascIdxFile
{
    public required CascIdxHeader Header { get; init; }
    public required IReadOnlyList<CascIdxEntry> Entries { get; init; }

    public static CascIdxFile Read(Stream stream)
    {
        ArgumentNullException.ThrowIfNull(stream);
        if (!stream.CanRead) throw new ArgumentException("Stream must be readable", nameof(stream));

        Span<byte> headerPrefix = stackalloc byte[0x28];
        ReadExactly(stream, headerPrefix);

        var headerHashSize = (int)BinaryPrimitives.ReadUInt32LittleEndian(headerPrefix[0x00..0x04]);
        var headerHash = BinaryPrimitives.ReadUInt32LittleEndian(headerPrefix[0x04..0x08]);
        var version = BinaryPrimitives.ReadUInt16LittleEndian(headerPrefix[0x08..0x0A]);
        var bucket = headerPrefix[0x0A];
        var extraBytes = headerPrefix[0x0B];

        var specSize = headerPrefix[0x0C];
        var specOffset = headerPrefix[0x0D];
        var specKey = headerPrefix[0x0E];
        var specOffsetBits = headerPrefix[0x0F];

        var archiveTotalSizeMax = BinaryPrimitives.ReadUInt64LittleEndian(headerPrefix[0x10..0x18]);

        // 0x18..0x20 is padding (8 bytes)
        var entriesSize = BinaryPrimitives.ReadUInt32LittleEndian(headerPrefix[0x20..0x24]);
        var entriesHash = BinaryPrimitives.ReadUInt32LittleEndian(headerPrefix[0x24..0x28]);

        var header = new CascIdxHeader
        {
            HeaderHashSize = headerHashSize,
            HeaderHash = headerHash,
            Version = version,
            BucketIndex = bucket,
            ExtraBytes = extraBytes,
            Spec = new CascIdxHeaderSpec(
                Size: specSize,
                Offset: specOffset,
                Key: specKey,
                OffsetBits: specOffsetBits),
            ArchiveTotalSizeMaximum = archiveTotalSizeMax,
            EntriesSize = entriesSize,
            EntriesHash = entriesHash,
        };

        if (header.RecordSize <= 0)
            throw new InvalidDataException("Invalid record size specced by header.");
        if (entriesSize % header.RecordSize != 0)
            throw new InvalidDataException("EntriesSize is not a multiple of record size.");

        var entryCount = (int)(entriesSize / (uint)header.RecordSize);
        byte[] entriesBytes = new byte[entriesSize];
        ReadExactly(stream, entriesBytes);

        var entries = new CascIdxEntry[entryCount];
        var offsetBits = header.Spec.OffsetBits;
        var archiveBits = header.Spec.Offset * 8 - offsetBits;
        ulong archiveMask = archiveBits == 64 ? ulong.MaxValue : ((1UL << archiveBits) - 1);
        ulong offsetMask = offsetBits == 64 ? ulong.MaxValue : ((1UL << offsetBits) - 1);

        for (int i = 0; i < entryCount; i++)
        {
            int recordStart = i * header.RecordSize;
            var record = entriesBytes.AsSpan(recordStart, header.RecordSize);

            var key = record[..header.Spec.Key].ToArray();
            var offsetBytes = record.Slice(header.Spec.Key, header.Spec.Offset);
            var raw = EndianBitConverter.ReadUIntBigEndian(offsetBytes);

            var archiveIndex = (int)((raw >> offsetBits) & archiveMask);
            var archiveOffset = (long)(raw & offsetMask);

            var sizeBytes = record.Slice(header.Spec.Key + header.Spec.Offset, header.Spec.Size);
            uint size = 0;
            for (int b = 0; b < sizeBytes.Length; b++)
                size |= (uint)sizeBytes[b] << (8 * b);

            entries[i] = new CascIdxEntry(key, archiveIndex, archiveOffset, size);
        }

        return new CascIdxFile { Header = header, Entries = entries };
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
