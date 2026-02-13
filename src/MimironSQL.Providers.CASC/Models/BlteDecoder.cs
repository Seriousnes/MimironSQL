using System.Buffers.Binary;
using System.Security.Cryptography;

using System.Buffers;

using LibDeflate;

using K4os.Compression.LZ4;

namespace MimironSQL.Providers;

/// <summary>
/// Decodes BLTE-encoded data blocks.
/// </summary>
internal static class BlteDecoder
{
    [ThreadStatic]
    private static ZlibDecompressor? t_zlibDecompressor;

    /// <summary>
    /// Decodes a BLTE buffer.
    /// </summary>
    /// <param name="blte">The BLTE bytes.</param>
    /// <param name="options">Optional decode options.</param>
    /// <returns>The decoded bytes.</returns>
    public static byte[] Decode(ReadOnlySpan<byte> blte, BlteDecodeOptions? options = null)
    {
        var span = blte;

        if (span.Length < 8)
            throw new InvalidDataException("BLTE buffer too small");

        if (span[0] != (byte)'B' || span[1] != (byte)'L' || span[2] != (byte)'T' || span[3] != (byte)'E')
            throw new InvalidDataException("Missing BLTE signature");

        var headerSize = BinaryPrimitives.ReadUInt32BigEndian(span[4..8]);
        if (headerSize != 0 && headerSize < 8)
            throw new InvalidDataException("Invalid BLTE headerSize");

        if (headerSize == 0)
        {
            // Single block: [mode][payload]
            return DecodeSingleBlock(span[8..], expectedMd5: [], blockIndex: 0, options?.OnSkippedBlock);
        }

        if (headerSize > span.Length)
            throw new InvalidDataException("BLTE header exceeds buffer length");

        var chunkInfo = span[8..(int)headerSize];
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

        // Parse chunk table once while computing total output size.
        // This avoids a second pass of repeated slicing and BigEndian reads.
        var blocks = new (uint RawSize, uint LogicalSize)[numBlocks];
        long totalLogicalSize = 0;
        int entryOffset = tableHeaderSize;

        for (int i = 0; i < numBlocks; i++)
        {
            uint rawSize = BinaryPrimitives.ReadUInt32BigEndian(chunkInfo.Slice(entryOffset, 4));
            if (rawSize == 0)
                throw new InvalidDataException("BLTE rawSize is zero");

            uint logicalSize = BinaryPrimitives.ReadUInt32BigEndian(chunkInfo.Slice(entryOffset + 4, 4));
            blocks[i] = (rawSize, logicalSize);

            totalLogicalSize += logicalSize;
            if (totalLogicalSize > int.MaxValue)
                throw new InvalidDataException("Decoded size too large");

            entryOffset += blockEntrySize;
        }

        var output = new byte[(int)totalLogicalSize];
        int dstOffset = 0;
        int dataOffset = checked((int)headerSize);
        ReadOnlySpan<byte> emptyMd5 = [];
        var onSkippedBlock = options?.OnSkippedBlock;

        for (int i = 0; i < numBlocks; i++)
        {
            var (rawSize, logicalSize) = blocks[i];
            int rawSizeInt = checked((int)rawSize);
            int logicalSizeInt = checked((int)logicalSize);

            if (dstOffset + logicalSizeInt > output.Length)
                throw new InvalidDataException("BLTE decoded data exceeds expected size");

            if (dataOffset + rawSizeInt > span.Length)
                throw new InvalidDataException("BLTE data truncated");

            DecodeBlockInto(span.Slice(dataOffset, rawSizeInt), output.AsSpan(dstOffset, logicalSizeInt), logicalSize, emptyMd5, blockIndex: i, onSkippedBlock);
            dataOffset += rawSizeInt;
            dstOffset += logicalSizeInt;
        }

        if (dstOffset != output.Length)
            throw new InvalidDataException("BLTE decoded size mismatch");

        return output;
    }

    private static byte[] DecodeSingleBlock(
        ReadOnlySpan<byte> encodedBlock,
        ReadOnlySpan<byte> expectedMd5,
        int blockIndex,
        Action<BlteSkippedBlock>? onSkippedBlock)
    {
        var span = encodedBlock;

        if (span.Length < 1)
            throw new InvalidDataException("BLTE block too small");

        if (!expectedMd5.IsEmpty)
        {
            Span<byte> actual = stackalloc byte[16];
            MD5.HashData(span, actual);
            if (!actual.SequenceEqual(expectedMd5))
                throw new InvalidDataException("BLTE block MD5 mismatch");
        }

        var mode = (char)span[0];
        var payload = encodedBlock[1..];

        if (mode == 'E')
        {
            onSkippedBlock?.Invoke(new BlteSkippedBlock(
                BlockIndex: blockIndex,
                RawSize: (uint)encodedBlock.Length,
                LogicalSize: 0,
                Mode: mode));

            // No keys available: preserve offsets by outputting zero-filled bytes.
            return [];
        }

        return mode switch
        {
            'N' => payload.ToArray(),
            'Z' => DecodeZlib(payload, logicalSizeHint: null),
            '4' => DecodeLz4Hc(payload, logicalSizeHint: null),
            'F' => throw new NotSupportedException("BLTE recursive blocks ('F') are not supported."),
            _ => throw new InvalidDataException($"Unsupported BLTE block mode '{mode}'"),
        };
    }

    private static void DecodeBlockInto(
        ReadOnlySpan<byte> encodedBlock,
        Span<byte> destination,
        uint logicalSizeHint,
        ReadOnlySpan<byte> expectedMd5,
        int blockIndex,
        Action<BlteSkippedBlock>? onSkippedBlock)
    {
        if (encodedBlock is { Length: < 1 })
            throw new InvalidDataException("BLTE block too small");

        if (!expectedMd5.IsEmpty)
        {
            Span<byte> actual = stackalloc byte[16];
            MD5.HashData(encodedBlock, actual);
            if (!actual.SequenceEqual(expectedMd5))
                throw new InvalidDataException("BLTE block MD5 mismatch");
        }

        var mode = (char)encodedBlock[0];
        var payload = encodedBlock[1..];

        if (mode == 'E')
        {
            onSkippedBlock?.Invoke(new BlteSkippedBlock(
                BlockIndex: blockIndex,
                RawSize: (uint)encodedBlock.Length,
                LogicalSize: logicalSizeHint,
                Mode: mode));

            destination.Clear();
            return;
        }

        switch (mode)
        {
            case 'N':
                {
                    if (payload.Length != destination.Length)
                        throw new InvalidDataException("BLTE uncompressed block size mismatch");

                    payload.CopyTo(destination);
                    return;
                }

            case 'Z':
                DecodeZlibInto(payload, destination);
                return;

            case '4':
                DecodeLz4HcInto(payload, destination, logicalSizeHint);
                return;

            case 'F':
                throw new NotSupportedException("BLTE recursive blocks ('F') are not supported.");

            default:
                throw new InvalidDataException($"Unsupported BLTE block mode '{mode}'");
        }
    }

    private static byte[] DecodeZlib(ReadOnlySpan<byte> payload, uint? logicalSizeHint)
    {
        var decompressor = t_zlibDecompressor ??= new ZlibDecompressor();

        if (logicalSizeHint is { } hint)
        {
            if (hint > int.MaxValue)
                throw new InvalidDataException("Decoded size too large");

            var output = new byte[(int)hint];
            var status = decompressor.Decompress(payload, output, out int bytesWritten, out _);

            if (status == OperationStatus.DestinationTooSmall)
                throw new InvalidDataException("BLTE zlib produced more data than expected");

            if (status != OperationStatus.Done)
                throw new InvalidDataException("BLTE zlib decoded size mismatch");

            if (bytesWritten == output.Length)
                return output;

            var trimmed = new byte[bytesWritten];
            output.AsSpan(0, bytesWritten).CopyTo(trimmed);
            return trimmed;
        }

        // Unknown output size: grow a rented buffer until decompress completes.
        byte[]? rented = null;
        int capacity = Math.Clamp(payload.Length * 4, 1_024, 1_048_576);

        try
        {
            while (true)
            {
                rented = ArrayPool<byte>.Shared.Rent(capacity);
                var dest = rented.AsSpan(0, capacity);

                var status = decompressor.Decompress(payload, dest, out int bytesWritten, out _);

                if (status == OperationStatus.DestinationTooSmall)
                {
                    ArrayPool<byte>.Shared.Return(rented);
                    rented = null;

                    capacity = checked(capacity * 2);
                    continue;
                }

                if (status != OperationStatus.Done)
                    throw new InvalidDataException("BLTE zlib decoded size mismatch");

                var output = new byte[bytesWritten];
                dest[..bytesWritten].CopyTo(output);
                return output;
            }
        }
        finally
        {
            if (rented is not null)
                ArrayPool<byte>.Shared.Return(rented);
        }
    }

    private static void DecodeZlibInto(ReadOnlySpan<byte> payload, Span<byte> destination)
    {
        var decompressor = t_zlibDecompressor ??= new ZlibDecompressor();

        var status = decompressor.Decompress(payload, destination, out int bytesWritten, out _);

        if (status == OperationStatus.DestinationTooSmall)
            throw new InvalidDataException("BLTE zlib produced more data than expected");

        if (status != OperationStatus.Done || bytesWritten != destination.Length)
            throw new InvalidDataException("BLTE zlib decoded size mismatch");

        // Preserve previous behavior: do not reject trailing bytes in the compressed payload.
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

    private static void DecodeLz4HcInto(ReadOnlySpan<byte> payload, Span<byte> destination, uint logicalSizeHint)
    {
        if (payload.Length < 1 + 8 + 1)
            throw new InvalidDataException("BLTE LZ4HC payload too small");

        byte headerVersion = payload[0];
        if (headerVersion != 1)
            throw new InvalidDataException($"Unsupported BLTE LZ4HC header version: {headerVersion}");

        ulong decodedSize64 = BinaryPrimitives.ReadUInt64BigEndian(payload[1..9]);
        if (decodedSize64 > int.MaxValue)
            throw new InvalidDataException("Decoded size too large");

        if (decodedSize64 != logicalSizeHint)
        {
            // Chunked BLTE provides a logical size in the table; we require it to match the payload header.
            throw new InvalidDataException("BLTE LZ4HC logical size mismatch");
        }

        int decodedSize = (int)decodedSize64;
        if (destination.Length != decodedSize)
            throw new InvalidDataException("BLTE LZ4HC destination size mismatch");

        int blockShift = payload[9];
        if (blockShift is < 5 or > 16)
            throw new InvalidDataException($"Invalid BLTE LZ4HC blockShift: {blockShift}");

        int blockSize = 1 << blockShift;

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

            int written = LZ4Codec.Decode(src, destination.Slice(dstOffset, expectedOut));
            if (written != expectedOut)
                throw new InvalidDataException("BLTE LZ4HC block decoded size mismatch");

            dstOffset += written;
        }
    }

    private static int ReadUInt24BigEndian(ReadOnlySpan<byte> bytes)
    {
        if (bytes.Length < 3) throw new ArgumentException("Need 3 bytes", nameof(bytes));
        return (bytes[0] << 16) | (bytes[1] << 8) | bytes[2];
    }
}
