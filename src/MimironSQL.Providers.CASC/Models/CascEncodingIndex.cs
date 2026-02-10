using System.Buffers.Binary;

namespace MimironSQL.Providers;

/// <summary>
/// Represents the decoded ENCODING index used to map content keys (CKey) to encoded keys (EKey).
/// </summary>
internal sealed class CascEncodingIndex
{
    private readonly Dictionary<CascKey, CascKey> _eKeyByCKey;

    private CascEncodingIndex(Dictionary<CascKey, CascKey> eKeyByCKey) => _eKeyByCKey = eKeyByCKey;

    /// <summary>
    /// Parses decoded ENCODING file bytes.
    /// </summary>
    /// <param name="decodedEncodingFile">Decoded ENCODING file bytes.</param>
    /// <returns>A parsed encoding index.</returns>
    public static CascEncodingIndex Parse(ReadOnlySpan<byte> decodedEncodingFile)
    {
        // Format described on wowdev.wiki (TACT: Encoding table).
        if (decodedEncodingFile.Length < 0x16)
            throw new InvalidDataException("ENCODING file too small");

        if (decodedEncodingFile[0] != (byte)'E' || decodedEncodingFile[1] != (byte)'N')
            throw new InvalidDataException("ENCODING signature not found");

        byte version = decodedEncodingFile[2];
        if (version != 1)
            throw new InvalidDataException($"Unsupported ENCODING version: {version}");

        int ckeySize = decodedEncodingFile[3];
        int ekeySize = decodedEncodingFile[4];
        if (ckeySize != CascKey.Length || ekeySize != CascKey.Length)
            throw new InvalidDataException($"Unsupported key sizes (ckey={ckeySize}, ekey={ekeySize})");

        int cPageSize = checked(BinaryPrimitives.ReadUInt16BigEndian(decodedEncodingFile[5..7]) * 1024);
        int ePageSize = checked(BinaryPrimitives.ReadUInt16BigEndian(decodedEncodingFile[7..9]) * 1024);
        int cPageCount = checked((int)BinaryPrimitives.ReadUInt32BigEndian(decodedEncodingFile[9..13]));
        int ePageCount = checked((int)BinaryPrimitives.ReadUInt32BigEndian(decodedEncodingFile[13..17]));

        byte flags = decodedEncodingFile[17];
        if (flags != 0)
        {
            // Not expected as of 2025-02, but tolerate unknown flags for now.
        }

        int eSpecSize = checked((int)BinaryPrimitives.ReadUInt32BigEndian(decodedEncodingFile[18..22]));

        int offset = 0x16;
        if (offset + eSpecSize > decodedEncodingFile.Length)
            throw new InvalidDataException("ENCODING ESpec block exceeds file size");

        offset += eSpecSize;

        // CEKeyPageTable
        int cIndexEntrySize = ckeySize + 16;
        int cIndexSize = checked(cPageCount * cIndexEntrySize);
        int cPagesSize = checked(cPageCount * cPageSize);

        if (offset + cIndexSize > decodedEncodingFile.Length)
            throw new InvalidDataException("ENCODING CEKey page index exceeds file size");

        offset += cIndexSize;

        if (offset + cPagesSize > decodedEncodingFile.Length)
            throw new InvalidDataException("ENCODING CEKey pages exceed file size");

        var map = new Dictionary<CascKey, CascKey>();

        for (int page = 0; page < cPageCount; page++)
        {
            var pageBytes = decodedEncodingFile.Slice(offset + (page * cPageSize), cPageSize);
            ParseCKeyPage(pageBytes, map);
        }

        // We currently ignore the EKeySpecPageTable and trailing encoding-spec strings.
        _ = ePageSize;
        _ = ePageCount;

        return new CascEncodingIndex(map);
    }

    /// <summary>
    /// Attempts to resolve an EKey for the provided CKey.
    /// </summary>
    /// <param name="ckey">The content key.</param>
    /// <param name="ekey">When this method returns, contains the resolved encoded key.</param>
    /// <returns><see langword="true"/> if an EKey was found; otherwise <see langword="false"/>.</returns>
    public bool TryGetEKey(CascKey ckey, out CascKey ekey) => _eKeyByCKey.TryGetValue(ckey, out ekey);

    /// <summary>
    /// Resolves an EKey for the provided CKey.
    /// </summary>
    /// <param name="ckey">The content key.</param>
    /// <returns>The resolved encoded key.</returns>
    public CascKey GetEKey(CascKey ckey)
    {
        if (!TryGetEKey(ckey, out var ekey))
            throw new KeyNotFoundException($"CKey not found in ENCODING: {ckey}");
        return ekey;
    }

    private static void ParseCKeyPage(ReadOnlySpan<byte> page, Dictionary<CascKey, CascKey> map)
    {
        int offset = 0;
        while (offset < page.Length)
        {
            // Fast padding detection.
            if (page[offset] == 0)
            {
                if (IsAllZero(page[offset..]))
                    return;
                offset++;
                continue;
            }

            if (offset + 1 + 5 + CascKey.Length > page.Length)
                return;

            byte keyCount = page[offset];
            if (keyCount == 0)
                return;

            offset += 1;

            // file_size (uint40 BE)
            offset += 5;

            var ckey = new CascKey(page.Slice(offset, CascKey.Length));
            offset += CascKey.Length;

            int ekeysBytes = checked(CascKey.Length * keyCount);
            if (offset + ekeysBytes > page.Length)
                return;

            var ekey = new CascKey(page.Slice(offset, CascKey.Length));
            offset += ekeysBytes;

            map.TryAdd(ckey, ekey);
        }
    }

    private static bool IsAllZero(ReadOnlySpan<byte> span)
    {
        for (int i = 0; i < span.Length; i++)
            if (span[i] != 0)
                return false;
        return true;
    }
}
