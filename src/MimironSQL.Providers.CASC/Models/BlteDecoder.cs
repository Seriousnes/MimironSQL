using System.Buffers.Binary;
using System.IO.Compression;
using System.Security.Cryptography;

using K4os.Compression.LZ4;

namespace MimironSQL.Providers;

public static class BlteDecoder
{
    public static byte[] Decode(ReadOnlySpan<byte> blte)
        => Decode(blte, options: null);

    public static byte[] Decode(ReadOnlySpan<byte> blte, BlteDecodeOptions? options)
    {
        if (blte.Length < 8)
            throw new InvalidDataException("BLTE buffer too small");

        if (blte[0] != (byte)'B' || blte[1] != (byte)'L' || blte[2] != (byte)'T' || blte[3] != (byte)'E')
            throw new InvalidDataException("Missing BLTE signature");

        var headerSize = BinaryPrimitives.ReadUInt32BigEndian(blte[4..8]);
        if (headerSize != 0 && headerSize < 8)
            throw new InvalidDataException("Invalid BLTE headerSize");

        if (headerSize == 0)
        {
            // Single block: [mode][payload]
            return DecodeBlock(blte[8..], logicalSizeHint: null, expectedMd5: null, blockIndex: 0, options?.OnSkippedBlock);
        }

        if (headerSize > blte.Length)
            throw new InvalidDataException("BLTE header exceeds buffer length");

        var chunkInfo = blte.Slice(8, checked((int)headerSize - 8));
        if (chunkInfo.Length < 5)
            throw new InvalidDataException("BLTE chunk table too small");

        var tableFormat = chunkInfo[0];
        if (tableFormat is not (0x0F or 0x10))
            throw new InvalidDataException($"Unsupported BLTE chunk table format: 0x{tableFormat:X2}");

        var numBlocks = ReadUInt24BigEndian(chunkInfo.Slice(1, 3));
        if (numBlocks <= 0)
            throw new InvalidDataException("BLTE chunk table has zero blocks");

        int blockEntrySize = tableFormat == 0x0F ? 24 : 40;
        int tableHeaderSize = 4;
        int required = tableHeaderSize + (numBlocks * blockEntrySize);
        if (chunkInfo.Length < required)
            throw new InvalidDataException("BLTE chunk table truncated");

        var blocks = new (uint rawSize, uint logicalSize, byte[] md5)[numBlocks];
        int offset = tableHeaderSize;
        for (int i = 0; i < numBlocks; i++)
        {
            uint rawSize = BinaryPrimitives.ReadUInt32BigEndian(chunkInfo.Slice(offset, 4));
            uint logicalSize = BinaryPrimitives.ReadUInt32BigEndian(chunkInfo.Slice(offset + 4, 4));
            var md5 = chunkInfo.Slice(offset + 8, 16).ToArray();
            blocks[i] = (rawSize, logicalSize, md5);
            offset += blockEntrySize;
        }

        int dataOffset = checked((int)headerSize);
        var output = new MemoryStream();

        for (int i = 0; i < numBlocks; i++)
        {
            var (rawSize, logicalSize, md5) = blocks[i];
            if (rawSize == 0)
                throw new InvalidDataException("BLTE rawSize is zero");

            int rawSizeInt = checked((int)rawSize);
            if (dataOffset + rawSizeInt > blte.Length)
                throw new InvalidDataException("BLTE data truncated");

            var encodedBlock = blte.Slice(dataOffset, rawSizeInt);
            dataOffset += rawSizeInt;

            var decoded = DecodeBlock(encodedBlock, logicalSize, md5, blockIndex: i, options?.OnSkippedBlock);
            output.Write(decoded, 0, decoded.Length);
        }

        return output.ToArray();
    }

    private static byte[] DecodeBlock(
        ReadOnlySpan<byte> encodedBlock,
        uint? logicalSizeHint,
        byte[]? expectedMd5,
        int blockIndex,
        Action<BlteSkippedBlock>? onSkippedBlock)
    {
        if (encodedBlock.Length < 1)
            throw new InvalidDataException("BLTE block too small");

        if (expectedMd5 is { } md5)
        {
            Span<byte> actual = stackalloc byte[16];
            MD5.HashData(encodedBlock, actual);
            if (!actual.SequenceEqual(md5))
                throw new InvalidDataException("BLTE block MD5 mismatch");
        }

        var mode = (char)encodedBlock[0];
        var payload = encodedBlock[1..];

        if (mode == 'E')
        {
            onSkippedBlock?.Invoke(new BlteSkippedBlock(
                BlockIndex: blockIndex,
                RawSize: (uint)encodedBlock.Length,
                LogicalSize: logicalSizeHint ?? 0,
                Mode: mode));

            // No keys available: preserve offsets by outputting zero-filled bytes.
            return logicalSizeHint is { } size
                ? new byte[checked((int)size)]
                : [];
        }

        return mode switch
        {
            'N' => payload.ToArray(),
            'Z' => DecodeZlib(payload, logicalSizeHint),
            '4' => DecodeLz4Hc(payload, logicalSizeHint),
            'F' => throw new NotSupportedException("BLTE recursive blocks ('F') are not supported."),
            _ => throw new InvalidDataException($"Unsupported BLTE block mode '{mode}'"),
        };
    }

    private static byte[] DecodeZlib(ReadOnlySpan<byte> payload, uint? logicalSizeHint)
    {
        using var input = new MemoryStream(payload.ToArray(), writable: false);
        using var z = new ZLibStream(input, CompressionMode.Decompress, leaveOpen: false);
        using var output = logicalSizeHint is { } s ? new MemoryStream(checked((int)s)) : new MemoryStream();
        z.CopyTo(output);
        return output.ToArray();
    }

    private static byte[] DecodeLz4Hc(ReadOnlySpan<byte> payload, uint? logicalSizeHint)
    {
        if (payload.Length < 1 + 8 + 1)
            throw new InvalidDataException("BLTE LZ4HC payload too small");

        byte headerVersion = payload[0];
        if (headerVersion != 1)
            throw new InvalidDataException($"Unsupported BLTE LZ4HC header version: {headerVersion}");

        ulong decodedSize64 = BinaryPrimitives.ReadUInt64BigEndian(payload[1..9]);
        if (decodedSize64 > int.MaxValue)
            throw new InvalidDataException("Decoded size too large");

        int decodedSize = (int)decodedSize64;
        if (logicalSizeHint is { } hint && hint != decodedSize64)
        {
            // Some producers may not match; prefer the payload header size.
        }

        int blockShift = payload[9];
        if (blockShift is < 5 or > 16)
            throw new InvalidDataException($"Invalid BLTE LZ4HC blockShift: {blockShift}");

        int blockSize = 1 << blockShift;
        var output = new byte[decodedSize];

        int srcOffset = 10;
        int dstOffset = 0;
        while (dstOffset < decodedSize)
        {
            if (srcOffset + 4 > payload.Length)
                throw new InvalidDataException("BLTE LZ4HC truncated (missing block size)");

            int compressedSize = BinaryPrimitives.ReadInt32BigEndian(payload.Slice(srcOffset, 4));
            srcOffset += 4;
            if (compressedSize <= 0)
                throw new InvalidDataException("BLTE LZ4HC invalid compressed block size");

            if (srcOffset + compressedSize > payload.Length)
                throw new InvalidDataException("BLTE LZ4HC truncated (missing block bytes)");

            int expectedOut = Math.Min(blockSize, decodedSize - dstOffset);
            var src = payload.Slice(srcOffset, compressedSize);
            srcOffset += compressedSize;

            int written = LZ4Codec.Decode(src, output.AsSpan(dstOffset, expectedOut));
            if (written != expectedOut)
                throw new InvalidDataException("BLTE LZ4HC block decoded size mismatch");

            dstOffset += written;
        }

        return output;
    }

    private static int ReadUInt24BigEndian(ReadOnlySpan<byte> bytes)
    {
        if (bytes.Length < 3) throw new ArgumentException("Need 3 bytes", nameof(bytes));
        return (bytes[0] << 16) | (bytes[1] << 8) | bytes[2];
    }
}
