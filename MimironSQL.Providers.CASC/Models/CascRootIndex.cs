using System.Buffers.Binary;

namespace MimironSQL.Providers;

internal sealed class CascRootIndex
{
    private const uint LocaleEnUs = 0x2;

    // Matches wowdev.wiki content_flags values (subset we care about).
    private const uint FlagLoadOnWindows = 0x8;
    private const uint FlagEncrypted = 0x0800_0000;
    private const uint FlagNoNameHash = 0x1000_0000;

    private readonly Dictionary<int, Entry> _bestByFileDataId;

    internal int EntryCount => _bestByFileDataId.Count;

    private CascRootIndex(Dictionary<int, Entry> bestByFileDataId) => _bestByFileDataId = bestByFileDataId;

    public bool TryGetContentKey(int fileDataId, out CascKey contentKey)
    {
        if (_bestByFileDataId.TryGetValue(fileDataId, out var entry))
        {
            contentKey = entry.ContentKey;
            return true;
        }

        contentKey = default;
        return false;
    }

    internal bool TryGetDebug(int fileDataId, out CascKey contentKey, out uint flags, out uint locale)
    {
        if (_bestByFileDataId.TryGetValue(fileDataId, out var entry))
        {
            contentKey = entry.ContentKey;
            flags = entry.Flags;
            locale = entry.Locale;
            return true;
        }

        contentKey = default;
        flags = 0;
        locale = 0;
        return false;
    }

    public static CascRootIndex Parse(ReadOnlySpan<byte> decodedRootFile)
    {
        if (decodedRootFile.Length < 4)
            throw new InvalidDataException("ROOT file too small.");

        // Modern WoW ROOT begins with TSFM. Some older tooling/documentation references MFST,
        // but the on-disk signature for WoW is typically TSFM.
        if (LooksLikeMfstHeader(decodedRootFile))
            return ParseMfst(decodedRootFile);

        if (decodedRootFile.Length >= 4 && TagEquals(decodedRootFile[..4], "TVFS"))
            throw new NotSupportedException("TVFS ROOT format detected. CASC.Net currently supports only TSFM/MFST ROOT parsing.");

        // Fallback for nonstandard/older layouts where the TSFM/MFST payload is embedded in a chunk stream.
        if (TryExtractMfstChunk(decodedRootFile, out var mfstBytes))
            return ParseMfst(mfstBytes);

        var preview = PreviewHex(decodedRootFile, 64);
        var sig = decodedRootFile.Length >= 4
            ? $"'{(char)decodedRootFile[0]}{(char)decodedRootFile[1]}{(char)decodedRootFile[2]}{(char)decodedRootFile[3]}'"
            : "<n/a>";

        throw new InvalidDataException($"Unsupported ROOT signature {sig}. Decoded header preview: {preview}");
    }

    private static string PreviewHex(ReadOnlySpan<byte> span, int maxBytes)
    {
        int len = Math.Min(span.Length, maxBytes);
        var bytes = span[..len].ToArray();
        return Convert.ToHexString(bytes).ToLowerInvariant();
    }

    private static int IndexOfTag(ReadOnlySpan<byte> span, string tag)
    {
        if (tag.Length != 4)
            throw new ArgumentException("Tag must be 4 chars.", nameof(tag));

        for (int i = 0; i + 4 <= span.Length; i++)
        {
            if (span[i] == (byte)tag[0] &&
                span[i + 1] == (byte)tag[1] &&
                span[i + 2] == (byte)tag[2] &&
                span[i + 3] == (byte)tag[3])
                return i;
        }

        return -1;
    }

    private static bool TryExtractMfstChunk(ReadOnlySpan<byte> decodedRootFile, out ReadOnlySpan<byte> mfstBytes)
    {
        // Some ROOT blobs may be chunked: [4-byte tag][u32 size LE][payload]...
        // If the blob already starts with TSFM/MFST, the caller should parse it directly.
        if (LooksLikeMfstHeader(decodedRootFile))
        {
            mfstBytes = decodedRootFile;
            return true;
        }

        // Chunked: [4-byte tag][u32 size LE][payload]...
        int offset = 0;
        while (offset + 8 <= decodedRootFile.Length)
        {
            var tag = decodedRootFile.Slice(offset, 4);
            var size = BinaryPrimitives.ReadUInt32LittleEndian(decodedRootFile.Slice(offset + 4, 4));
            offset += 8;

            if (size > int.MaxValue || offset + (int)size > decodedRootFile.Length)
                break;

            var payload = decodedRootFile.Slice(offset, (int)size);
            offset += (int)size;

            if (TagEquals(tag, "MFST"))
            {
                mfstBytes = payload;
                return true;
            }

            // Some ROOT containers embed the TSFM/MFST payload inside a TSFM-tagged chunk.
            if (TagEquals(tag, "TSFM") && LooksLikeMfstHeader(payload))
            {
                mfstBytes = payload;
                return true;
            }
        }

        mfstBytes = default;
        return false;
    }

    private static bool LooksLikeMfstHeader(ReadOnlySpan<byte> span)
        => span.Length >= 4 && (TagEquals(span[..4], "MFST") || TagEquals(span[..4], "TSFM"));

    private static bool TagEquals(ReadOnlySpan<byte> tagBytes, string tag)
        => tagBytes.Length == 4 &&
           tagBytes[0] == (byte)tag[0] &&
           tagBytes[1] == (byte)tag[1] &&
           tagBytes[2] == (byte)tag[2] &&
           tagBytes[3] == (byte)tag[3];

    private static CascRootIndex ParseMfst(ReadOnlySpan<byte> mfst)
    {
        // This implements the deterministic TSFM parsing approach used by CascLib:
        // - Detect header variant (50893+ includes SizeOfHeader + Version; older 30080+ includes totals only)
        // - Per-group header size depends on TSFM header Version:
        //   * v0/v1: 12 bytes [NumberOfFiles][ContentFlags][LocaleFlags]
        //   * v2: 17 bytes (packed) [NumberOfFiles][LocaleFlags][Flags1][Flags2][Flags3:u8]
        // - FileDataIds are deltas; fileDataId is reconstructed as:
        //   fileDataId += delta; emit; fileDataId++

        int offset = 0;

        if (!LooksLikeMfstHeader(mfst))
            throw new InvalidDataException("TSFM header signature not found.");

        offset += 4; // Signature

        uint rootVersion;
        bool allowNonNamedFiles;

        // Prefer 50893+ header if it looks valid.
        if (mfst.Length >= 20)
        {
            uint sizeOfHeader = BinaryPrimitives.ReadUInt32LittleEndian(mfst.Slice(offset, 4));
            uint version = BinaryPrimitives.ReadUInt32LittleEndian(mfst.Slice(offset + 4, 4));
            uint totalFiles = BinaryPrimitives.ReadUInt32LittleEndian(mfst.Slice(offset + 8, 4));
            uint filesWithNameHash = BinaryPrimitives.ReadUInt32LittleEndian(mfst.Slice(offset + 12, 4));

            bool headerLooksValid =
                (version == 1 || version == 2) &&
                filesWithNameHash <= totalFiles &&
                sizeOfHeader >= 4 &&
                sizeOfHeader <= mfst.Length;

            if (headerLooksValid)
            {
                rootVersion = version;
                allowNonNamedFiles = totalFiles != filesWithNameHash;
                offset = checked((int)sizeOfHeader);
            }
            else
            {
                // Fall through to 30080 header
                rootVersion = 0;
                allowNonNamedFiles = false;
            }
        }
        else
        {
            rootVersion = 0;
            allowNonNamedFiles = false;
        }

        // 30080 header: [Signature][TotalFiles][FilesWithNameHash]
        if (offset == 4)
        {
            if (mfst.Length < 12)
                throw new InvalidDataException("TSFM header too small.");

            uint totalFiles = BinaryPrimitives.ReadUInt32LittleEndian(mfst.Slice(4, 4));
            uint filesWithNameHash = BinaryPrimitives.ReadUInt32LittleEndian(mfst.Slice(8, 4));
            if (filesWithNameHash > totalFiles)
                throw new InvalidDataException("Invalid TSFM header (named files exceed total files).");

            allowNonNamedFiles = totalFiles != filesWithNameHash;
            rootVersion = 0;
            offset = 12;
        }

        var best = new Dictionary<int, Entry>();

        while (offset < mfst.Length)
        {
            if (offset + 4 > mfst.Length)
                break;

            uint numberOfFiles = BinaryPrimitives.ReadUInt32LittleEndian(mfst.Slice(offset, 4));
            offset += 4;

            if (numberOfFiles == 0)
                break;

            if (numberOfFiles > int.MaxValue)
                break;

            int recordCount = (int)numberOfFiles;

            uint flags;
            uint locale;

            if (rootVersion == 2)
            {
                // Packed group header (build 58221+):
                // [u32 NumberOfFiles][u32 LocaleFlags][u32 Flags1][u32 Flags2][u8 Flags3]
                if (offset + 13 > mfst.Length)
                    break;

                locale = BinaryPrimitives.ReadUInt32LittleEndian(mfst.Slice(offset, 4));
                uint flags1 = BinaryPrimitives.ReadUInt32LittleEndian(mfst.Slice(offset + 4, 4));
                uint flags2 = BinaryPrimitives.ReadUInt32LittleEndian(mfst.Slice(offset + 8, 4));
                byte flags3 = mfst[offset + 12];
                offset += 13;

                flags = flags1 | flags2 | ((uint)flags3 << 17);
            }
            else
            {
                // Legacy group header: [u32 ContentFlags][u32 LocaleFlags]
                if (offset + 8 > mfst.Length)
                    break;

                flags = BinaryPrimitives.ReadUInt32LittleEndian(mfst.Slice(offset, 4));
                locale = BinaryPrimitives.ReadUInt32LittleEndian(mfst.Slice(offset + 4, 4));
                offset += 8;
            }

            // FileDataId delta array.
            long deltasBytes = (long)recordCount * 4;
            if (offset + deltasBytes > mfst.Length)
                break;

            var deltas = mfst.Slice(offset, (int)deltasBytes);
            offset += (int)deltasBytes;

            // Content keys.
            long ckeyBytes = (long)recordCount * CascKey.Length;
            if (offset + ckeyBytes > mfst.Length)
                break;

            var contentKeys = mfst.Slice(offset, (int)ckeyBytes);
            offset += (int)ckeyBytes;

            // Optional name hashes (present unless NO_NAME_HASH flag is set).
            bool hasNameHashes = (flags & FlagNoNameHash) == 0;
            if (hasNameHashes)
            {
                long nameHashBytes = (long)recordCount * 8;
                if (offset + nameHashBytes > mfst.Length)
                    break;
                offset += (int)nameHashBytes;
            }

            // Reconstruct FileDataIds.
            uint fileDataId = 0;
            for (int i = 0; i < recordCount; i++)
            {
                uint delta = BinaryPrimitives.ReadUInt32LittleEndian(deltas.Slice(i * 4, 4));
                fileDataId += delta;

                if (fileDataId > int.MaxValue)
                    break;

                int fdid = (int)fileDataId;

                var ckeySpan = contentKeys.Slice(i * CascKey.Length, CascKey.Length);
                var ckey = new CascKey(ckeySpan);

                var candidate = new Entry(ckey, flags, locale);
                if (!best.TryGetValue(fdid, out var existing) || candidate.IsBetterThan(existing))
                    best[fdid] = candidate;

                // CascLib advances by one after each record.
                fileDataId++;
            }

            _ = allowNonNamedFiles;
        }

        return new CascRootIndex(best);
    }

    private readonly record struct Entry(CascKey ContentKey, uint Flags, uint Locale)
    {
        public bool IsBetterThan(Entry other)
        {
            // Priority: non-encrypted, then enUS, then LoadOnWindows.
            int score = 0;
            if ((Flags & FlagEncrypted) != 0)
                score += 1000;
            if ((Locale & LocaleEnUs) == 0)
                score += 100;
            if ((Flags & FlagLoadOnWindows) == 0)
                score += 10;

            int otherScore = 0;
            if ((other.Flags & FlagEncrypted) != 0)
                otherScore += 1000;
            if ((other.Locale & LocaleEnUs) == 0)
                otherScore += 100;
            if ((other.Flags & FlagLoadOnWindows) == 0)
                otherScore += 10;

            return score < otherScore;
        }
    }
}
