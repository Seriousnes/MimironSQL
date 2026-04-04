using System.Buffers.Binary;
using System.Security.Cryptography;

using System.Buffers;

using LibDeflate;

using K4os.Compression.LZ4;

using Security.Cryptography;

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

        var optionsOrDefault = options ?? new BlteDecodeOptions();

        if (span.Length < 8)
        {
            throw new InvalidDataException("BLTE buffer too small");
        }

        if (span[0] != (byte)'B' || span[1] != (byte)'L' || span[2] != (byte)'T' || span[3] != (byte)'E')
        {
            throw new InvalidDataException("Missing BLTE signature");
        }

        var headerSize = BinaryPrimitives.ReadUInt32BigEndian(span[4..8]);
        if (headerSize != 0 && headerSize < 8)
        {
            throw new InvalidDataException("Invalid BLTE headerSize");
        }

        if (headerSize == 0)
        {
            // Single block: [mode][payload]
            return DecodeSingleBlock(span[8..], expectedMd5: [], blockIndex: 0, optionsOrDefault);
        }

        if (headerSize > span.Length)
        {
            throw new InvalidDataException("BLTE header exceeds buffer length");
        }

        var chunkInfo = span[8..(int)headerSize];
        if (chunkInfo.Length < 5)
        {
            throw new InvalidDataException("BLTE chunk table too small");
        }

        var tableFormat = chunkInfo[0];
        if (tableFormat is not (0x0F or 0x10))
        {
            throw new InvalidDataException($"Unsupported BLTE chunk table format: 0x{tableFormat:X2}");
        }

        var numBlocks = ReadUInt24BigEndian(chunkInfo.Slice(1, 3));
        if (numBlocks <= 0)
        {
            throw new InvalidDataException("BLTE chunk table has zero blocks");
        }

        int blockEntrySize = tableFormat == 0x0F ? 24 : 40;
        int tableHeaderSize = 4;
        int required = tableHeaderSize + (numBlocks * blockEntrySize);
        if (chunkInfo.Length < required)
        {
            throw new InvalidDataException("BLTE chunk table truncated");
        }

        // Parse chunk table once while computing total output size.
        // This avoids a second pass of repeated slicing and BigEndian reads.
        var blocks = new (uint RawSize, uint LogicalSize)[numBlocks];
        long totalLogicalSize = 0;
        int entryOffset = tableHeaderSize;

        for (int i = 0; i < numBlocks; i++)
        {
            uint rawSize = BinaryPrimitives.ReadUInt32BigEndian(chunkInfo.Slice(entryOffset, 4));
            if (rawSize == 0)
            {
                throw new InvalidDataException("BLTE rawSize is zero");
            }

            uint logicalSize = BinaryPrimitives.ReadUInt32BigEndian(chunkInfo.Slice(entryOffset + 4, 4));
            blocks[i] = (rawSize, logicalSize);

            totalLogicalSize += logicalSize;
            if (totalLogicalSize > int.MaxValue)
            {
                throw new InvalidDataException("Decoded size too large");
            }

            entryOffset += blockEntrySize;
        }

        var output = new byte[(int)totalLogicalSize];
        int dstOffset = 0;
        int dataOffset = checked((int)headerSize);
        ReadOnlySpan<byte> emptyMd5 = [];
        // optionsOrDefault already computed above.

        for (int i = 0; i < numBlocks; i++)
        {
            var (rawSize, logicalSize) = blocks[i];
            int rawSizeInt = checked((int)rawSize);
            int logicalSizeInt = checked((int)logicalSize);

            if (dstOffset + logicalSizeInt > output.Length)
            {
                throw new InvalidDataException("BLTE decoded data exceeds expected size");
            }

            if (dataOffset + rawSizeInt > span.Length)
            {
                throw new InvalidDataException("BLTE data truncated");
            }

            DecodeBlockInto(span.Slice(dataOffset, rawSizeInt), output.AsSpan(dstOffset, logicalSizeInt), logicalSize, emptyMd5, blockIndex: i, optionsOrDefault);
            dataOffset += rawSizeInt;
            dstOffset += logicalSizeInt;
        }

        if (dstOffset != output.Length)
        {
            throw new InvalidDataException("BLTE decoded size mismatch");
        }

        return output;
    }

    private static byte[] DecodeSingleBlock(
        ReadOnlySpan<byte> encodedBlock,
        ReadOnlySpan<byte> expectedMd5,
        int blockIndex,
        BlteDecodeOptions options)
    {
        var span = encodedBlock;

        if (span.Length < 1)
        {
            throw new InvalidDataException("BLTE block too small");
        }

        if (!expectedMd5.IsEmpty)
        {
            Span<byte> actual = stackalloc byte[16];
            MD5.HashData(span, actual);
            if (!actual.SequenceEqual(expectedMd5))
            {
                throw new InvalidDataException("BLTE block MD5 mismatch");
            }
        }

        var mode = (char)span[0];
        var payload = encodedBlock[1..];

        if (mode == 'E')
        {
            return DecodeEncryptedSingleBlock(payload, blockIndex, options);
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
        BlteDecodeOptions options)
    {
        if (encodedBlock is { Length: < 1 })
        {
            throw new InvalidDataException("BLTE block too small");
        }

        if (!expectedMd5.IsEmpty)
        {
            Span<byte> actual = stackalloc byte[16];
            MD5.HashData(encodedBlock, actual);
            if (!actual.SequenceEqual(expectedMd5))
            {
                throw new InvalidDataException("BLTE block MD5 mismatch");
            }
        }

        var mode = (char)encodedBlock[0];
        var payload = encodedBlock[1..];

        if (mode == 'E')
        {
            DecodeEncryptedBlockInto(payload, destination, logicalSizeHint, blockIndex, options);
            return;
        }

        switch (mode)
        {
            case 'N':
                {
                    if (payload.Length != destination.Length)
                    {
                        throw new InvalidDataException("BLTE uncompressed block size mismatch");
                    }

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

    private static byte[] DecodeEncryptedSingleBlock(
        ReadOnlySpan<byte> payload,
        int blockIndex,
        BlteDecodeOptions options)
    {
        if (!TryParseEncryptedBlock(payload, out var keyName, out var iv, out var type, out var encryptedData))
        {
            throw new InvalidDataException("BLTE encrypted block payload was malformed.");
        }

        if (type is not 'S')
        {
            throw new NotSupportedException($"BLTE encrypted block type '{type}' is not supported (only 'S' salsa20 is supported).");
        }

        if (!TryResolveTactKey(keyName, blockIndex, options, out var key, out var shouldSkip))
        {
            if (shouldSkip)
            {
                return [];
            }

            throw new InvalidOperationException("Failed to resolve a TACT key for an encrypted BLTE block.");
        }

        var decrypted = new byte[encryptedData.Length];
        Span<byte> nonce = stackalloc byte[8];

        FillNonce(iv, blockIndex, nonce);
        using (var salsa20 = new Salsa20(key.Span, nonce))
        {
            salsa20.Transform(encryptedData, decrypted);
        }

        var innerMode = (char)(decrypted.Length > 0 ? decrypted[0] : (byte)0);
        if (innerMode is 'E')
        {
            throw new InvalidDataException("BLTE encrypted block decrypted to another encrypted block; this is not supported.");
        }

        try
        {
            return DecodeSingleBlock(decrypted, expectedMd5: [], blockIndex, options);
        }
        catch (Exception ex)
        {
            throw new InvalidDataException(
                $"BLTE encrypted block decrypted but inner BLTE could not be decoded. KeyName={Convert.ToHexString(keyName)} IV={Convert.ToHexString(iv)} BlockIndex={blockIndex} Nonce={Convert.ToHexString(nonce)} InnerMode={(innerMode == '\0' ? "(none)" : $"'{innerMode}' (0x{(byte)innerMode:X2})")}",
                ex);
        }
    }

    private static void DecodeEncryptedBlockInto(
        ReadOnlySpan<byte> payload,
        Span<byte> destination,
        uint logicalSizeHint,
        int blockIndex,
        BlteDecodeOptions options)
    {
        if (!TryParseEncryptedBlock(payload, out var keyName, out var iv, out var type, out var encryptedData))
        {
            throw new InvalidDataException("BLTE encrypted block payload was malformed.");
        }

        if (type is not 'S')
        {
            throw new NotSupportedException($"BLTE encrypted block type '{type}' is not supported (only 'S' salsa20 is supported).");
        }

        if (!TryResolveTactKey(keyName, blockIndex, options, out var key, out var shouldSkip))
        {
            if (shouldSkip)
            {
                destination.Clear();
                return;
            }

            throw new InvalidOperationException("Failed to resolve a TACT key for an encrypted BLTE block.");
        }

        var decrypted = new byte[encryptedData.Length];
        Span<byte> nonce = stackalloc byte[8];

        FillNonce(iv, blockIndex, nonce);
        using (var salsa20 = new Salsa20(key.Span, nonce))
        {
            salsa20.Transform(encryptedData, decrypted);
        }

        var innerMode = (char)(decrypted.Length > 0 ? decrypted[0] : (byte)0);
        if (innerMode is 'E')
        {
            throw new InvalidDataException("BLTE encrypted block decrypted to another encrypted block; this is not supported.");
        }

        try
        {
            DecodeBlockInto(decrypted, destination, logicalSizeHint, expectedMd5: [], blockIndex, options);
        }
        catch (Exception ex)
        {
            throw new InvalidDataException(
                $"BLTE encrypted block decrypted but inner BLTE could not be decoded. KeyName={Convert.ToHexString(keyName)} IV={Convert.ToHexString(iv)} BlockIndex={blockIndex} Nonce={Convert.ToHexString(nonce)} InnerMode={(innerMode == '\0' ? "(none)" : $"'{innerMode}' (0x{(byte)innerMode:X2})")}",
                ex);
        }
    }

    private static bool TryParseEncryptedBlock(
        ReadOnlySpan<byte> payload,
        out ReadOnlySpan<byte> keyName,
        out ReadOnlySpan<byte> iv,
        out char type,
        out ReadOnlySpan<byte> encryptedData)
    {
        keyName = default;
        iv = default;
        type = default;
        encryptedData = default;

        if (payload.Length < 1)
        {
            return false;
        }

        var keyNameLength = payload[0];
        if (keyNameLength != 8)
        {
            throw new InvalidDataException($"BLTE encrypted block key_name_length was {keyNameLength} (expected 8).");
        }

        var keyNameStart = 1;
        var keyNameEnd = keyNameStart + keyNameLength;
        if (payload.Length < keyNameEnd + 1)
        {
            return false;
        }

        keyName = payload.Slice(keyNameStart, keyNameLength);

        var ivLength = payload[keyNameEnd];
        if (ivLength is not (4 or 8))
        {
            throw new InvalidDataException($"BLTE encrypted block IV_length was {ivLength} (expected 4 or 8).");
        }

        var ivStart = keyNameEnd + 1;
        var ivEnd = ivStart + ivLength;
        if (payload.Length < ivEnd + 1)
        {
            return false;
        }

        iv = payload.Slice(ivStart, ivLength);
        type = (char)payload[ivEnd];
        encryptedData = payload[(ivEnd + 1)..];
        return true;
    }

    private static bool TryResolveTactKey(
        ReadOnlySpan<byte> keyName,
        int blockIndex,
        BlteDecodeOptions options,
        out ReadOnlyMemory<byte> key,
        out bool shouldSkip)
    {
        key = default;
        shouldSkip = false;

        var tactKeyProvider = options.TactKeyProvider;
        if (tactKeyProvider is null)
        {
            if (!options.ThrowOnEncryptedBlockWithoutKey)
            {
                options.OnSkippedBlock?.Invoke(new BlteSkippedBlock(
                    BlockIndex: blockIndex,
                    RawSize: (uint)(keyName.Length + 1),
                    LogicalSize: 0,
                    Mode: 'E'));
                shouldSkip = true;
                return false;
            }

            throw new InvalidOperationException("Encountered an encrypted BLTE block but no ITactKeyProvider was configured.");
        }

        ulong lookupBe = BinaryPrimitives.ReadUInt64BigEndian(keyName);
        ulong lookupLe = BinaryPrimitives.ReadUInt64LittleEndian(keyName);

        if (tactKeyProvider.TryGetKey(lookupBe, out key) || tactKeyProvider.TryGetKey(lookupLe, out key))
        {
            return true;
        }

        if (!options.ThrowOnEncryptedBlockWithoutKey)
        {
            options.OnSkippedBlock?.Invoke(new BlteSkippedBlock(
                BlockIndex: blockIndex,
                RawSize: (uint)(keyName.Length + 1),
                LogicalSize: 0,
                Mode: 'E'));
            shouldSkip = true;
            return false;
        }

        throw new KeyNotFoundException($"Missing TACT key for BLTE key_name {Convert.ToHexString(keyName)} (lookups tried: 0x{lookupBe:X16}, 0x{lookupLe:X16}).");
    }

    private static void FillNonce(ReadOnlySpan<byte> iv, int blockIndex, Span<byte> nonce)
    {
        nonce.Clear();

        if (iv.Length is not (4 or 8))
        {
            throw new ArgumentOutOfRangeException(nameof(iv), $"BLTE encrypted IV must be 4 or 8 bytes, but was {iv.Length}.");
        }

        // CascLib behavior:
        // - If IV is 4 bytes, pad to 8 by appending zeros.
        // - XOR the block index into the first 4 bytes (little-endian).
        iv.CopyTo(nonce);

        Span<byte> indexBytes = stackalloc byte[4];
        BinaryPrimitives.WriteInt32LittleEndian(indexBytes, blockIndex);

        nonce[0] ^= indexBytes[0];
        nonce[1] ^= indexBytes[1];
        nonce[2] ^= indexBytes[2];
        nonce[3] ^= indexBytes[3];
    }

    private static byte[] DecodeZlib(ReadOnlySpan<byte> payload, uint? logicalSizeHint)
    {
        var decompressor = t_zlibDecompressor ??= new ZlibDecompressor();

        if (logicalSizeHint is { } hint)
        {
            if (hint > int.MaxValue)
            {
                throw new InvalidDataException("Decoded size too large");
            }

            var output = new byte[(int)hint];
            var status = decompressor.Decompress(payload, output, out int bytesWritten, out _);

            if (status == OperationStatus.DestinationTooSmall)
            {
                throw new InvalidDataException("BLTE zlib produced more data than expected");
            }

            if (status != OperationStatus.Done)
            {
                throw new InvalidDataException("BLTE zlib decoded size mismatch");
            }

            if (bytesWritten == output.Length)
            {
                return output;
            }

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
                {
                    throw new InvalidDataException("BLTE zlib decoded size mismatch");
                }

                var output = new byte[bytesWritten];
                dest[..bytesWritten].CopyTo(output);
                return output;
            }
        }
        finally
        {
            if (rented is not null)
            {
                ArrayPool<byte>.Shared.Return(rented);
            }
        }
    }

    private static void DecodeZlibInto(ReadOnlySpan<byte> payload, Span<byte> destination)
    {
        var decompressor = t_zlibDecompressor ??= new ZlibDecompressor();

        var status = decompressor.Decompress(payload, destination, out int bytesWritten, out _);

        if (status == OperationStatus.DestinationTooSmall)
        {
            throw new InvalidDataException("BLTE zlib produced more data than expected");
        }

        if (status != OperationStatus.Done || bytesWritten != destination.Length)
        {
            throw new InvalidDataException("BLTE zlib decoded size mismatch");
        }

        // Preserve previous behavior: do not reject trailing bytes in the compressed payload.
    }

    private static byte[] DecodeLz4Hc(ReadOnlySpan<byte> payload, uint? logicalSizeHint)
    {
        if (payload.Length < 1 + 8 + 1)
        {
            throw new InvalidDataException("BLTE LZ4HC payload too small");
        }

        byte headerVersion = payload[0];
        if (headerVersion != 1)
        {
            throw new InvalidDataException($"Unsupported BLTE LZ4HC header version: {headerVersion}");
        }

        ulong decodedSize64 = BinaryPrimitives.ReadUInt64BigEndian(payload[1..9]);
        if (decodedSize64 > int.MaxValue)
        {
            throw new InvalidDataException("Decoded size too large");
        }

        int decodedSize = (int)decodedSize64;
        if (logicalSizeHint is { } hint && hint != decodedSize64)
        {
            // Some producers may not match; prefer the payload header size.
        }

        int blockShift = payload[9];
        if (blockShift is < 5 or > 16)
        {
            throw new InvalidDataException($"Invalid BLTE LZ4HC blockShift: {blockShift}");
        }

        int blockSize = 1 << blockShift;
        var output = new byte[decodedSize];

        int srcOffset = 10;
        int dstOffset = 0;
        while (dstOffset < decodedSize)
        {
            if (srcOffset + 4 > payload.Length)
            {
                throw new InvalidDataException("BLTE LZ4HC truncated (missing block size)");
            }

            int compressedSize = BinaryPrimitives.ReadInt32BigEndian(payload.Slice(srcOffset, 4));
            srcOffset += 4;
            if (compressedSize <= 0)
            {
                throw new InvalidDataException("BLTE LZ4HC invalid compressed block size");
            }

            if (srcOffset + compressedSize > payload.Length)
            {
                throw new InvalidDataException("BLTE LZ4HC truncated (missing block bytes)");
            }

            int expectedOut = Math.Min(blockSize, decodedSize - dstOffset);
            var src = payload.Slice(srcOffset, compressedSize);
            srcOffset += compressedSize;

            int written = LZ4Codec.Decode(src, output.AsSpan(dstOffset, expectedOut));
            if (written != expectedOut)
            {
                throw new InvalidDataException("BLTE LZ4HC block decoded size mismatch");
            }

            dstOffset += written;
        }

        return output;
    }

    private static void DecodeLz4HcInto(ReadOnlySpan<byte> payload, Span<byte> destination, uint logicalSizeHint)
    {
        if (payload.Length < 1 + 8 + 1)
        {
            throw new InvalidDataException("BLTE LZ4HC payload too small");
        }

        byte headerVersion = payload[0];
        if (headerVersion != 1)
        {
            throw new InvalidDataException($"Unsupported BLTE LZ4HC header version: {headerVersion}");
        }

        ulong decodedSize64 = BinaryPrimitives.ReadUInt64BigEndian(payload[1..9]);
        if (decodedSize64 > int.MaxValue)
        {
            throw new InvalidDataException("Decoded size too large");
        }

        if (decodedSize64 != logicalSizeHint)
        {
            // Chunked BLTE provides a logical size in the table; we require it to match the payload header.
            throw new InvalidDataException("BLTE LZ4HC logical size mismatch");
        }

        int decodedSize = (int)decodedSize64;
        if (destination.Length != decodedSize)
        {
            throw new InvalidDataException("BLTE LZ4HC destination size mismatch");
        }

        int blockShift = payload[9];
        if (blockShift is < 5 or > 16)
        {
            throw new InvalidDataException($"Invalid BLTE LZ4HC blockShift: {blockShift}");
        }

        int blockSize = 1 << blockShift;

        int srcOffset = 10;
        int dstOffset = 0;
        while (dstOffset < decodedSize)
        {
            if (srcOffset + 4 > payload.Length)
            {
                throw new InvalidDataException("BLTE LZ4HC truncated (missing block size)");
            }

            int compressedSize = BinaryPrimitives.ReadInt32BigEndian(payload.Slice(srcOffset, 4));
            srcOffset += 4;
            if (compressedSize <= 0)
            {
                throw new InvalidDataException("BLTE LZ4HC invalid compressed block size");
            }

            if (srcOffset + compressedSize > payload.Length)
            {
                throw new InvalidDataException("BLTE LZ4HC truncated (missing block bytes)");
            }

            int expectedOut = Math.Min(blockSize, decodedSize - dstOffset);
            var src = payload.Slice(srcOffset, compressedSize);
            srcOffset += compressedSize;

            int written = LZ4Codec.Decode(src, destination.Slice(dstOffset, expectedOut));
            if (written != expectedOut)
            {
                throw new InvalidDataException("BLTE LZ4HC block decoded size mismatch");
            }

            dstOffset += written;
        }
    }

    private static int ReadUInt24BigEndian(ReadOnlySpan<byte> bytes)
    {
        if (bytes.Length < 3)
        {
            throw new ArgumentException("Need 3 bytes", nameof(bytes));
        }

        return (bytes[0] << 16) | (bytes[1] << 8) | bytes[2];
    }
}
