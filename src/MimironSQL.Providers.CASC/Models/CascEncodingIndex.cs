using System.Buffers.Binary;
using System.Collections.Concurrent;

namespace MimironSQL.Providers;

/// <summary>
/// Represents the decoded ENCODING index used to map content keys (CKey) to encoded keys (EKey).
/// </summary>
internal sealed class CascEncodingIndex
{
    private readonly byte[] _decodedEncodingFile;
    private readonly int _cPageSize;
    private readonly int _cPageCount;
    private readonly int _cPagesOffset;

    // Cache only keys we've actually resolved in this process.
    private readonly ConcurrentDictionary<CascKey, CascKey> _resolved = new();

    private CascEncodingIndex(byte[] decodedEncodingFile, int cPageSize, int cPageCount, int cPagesOffset)
    {
        _decodedEncodingFile = decodedEncodingFile;
        _cPageSize = cPageSize;
        _cPageCount = cPageCount;
        _cPagesOffset = cPagesOffset;
    }

    /// <summary>
    /// Parses decoded ENCODING file bytes.
    /// </summary>
    /// <param name="decodedEncodingFile">Decoded ENCODING file bytes.</param>
    /// <returns>A parsed encoding index.</returns>
    public static CascEncodingIndex Parse(byte[] decodedEncodingFile)
    {
        ArgumentNullException.ThrowIfNull(decodedEncodingFile);

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

        int cPageSize = checked(BinaryPrimitives.ReadUInt16BigEndian(decodedEncodingFile.AsSpan(5, 2)) * 1024);
        int cPageCount = checked((int)BinaryPrimitives.ReadUInt32BigEndian(decodedEncodingFile.AsSpan(9, 4)));

        byte flags = decodedEncodingFile[17];
        if (flags != 0)
        {
            // Not expected as of 2025-02, but tolerate unknown flags for now.
        }

        int eSpecSize = checked((int)BinaryPrimitives.ReadUInt32BigEndian(decodedEncodingFile.AsSpan(18, 4)));

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

        // Do not eagerly materialize the full CKey->EKey map. We'll scan pages on-demand.
        var cPagesOffset = offset;

        // We currently ignore the EKeySpecPageTable and trailing encoding-spec strings.

        return new CascEncodingIndex(decodedEncodingFile, cPageSize, cPageCount, cPagesOffset);
    }

    public static CascEncodingIndex Parse(ReadOnlySpan<byte> decodedEncodingFile)
        => Parse(decodedEncodingFile.ToArray());

    /// <summary>
    /// Attempts to resolve an EKey for the provided CKey.
    /// </summary>
    /// <param name="ckey">The content key.</param>
    /// <param name="ekey">When this method returns, contains the resolved encoded key.</param>
    /// <returns><see langword="true"/> if an EKey was found; otherwise <see langword="false"/>.</returns>
    public bool TryGetEKey(CascKey ckey, out CascKey ekey)
    {
        if (_resolved.TryGetValue(ckey, out ekey))
            return true;

        if (!TryResolveFromPages(ckey, out ekey))
        {
            ekey = default;
            return false;
        }

        _resolved.TryAdd(ckey, ekey);
        return true;
    }

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

    private bool TryResolveFromPages(CascKey targetCKey, out CascKey ekey)
    {
        // Simple, low-memory strategy: scan CKey pages until we find a match.
        // In practice we only resolve a relatively small number of keys per process.
        for (int pageIndex = 0; pageIndex < _cPageCount; pageIndex++)
        {
            var pageOffset = _cPagesOffset + (pageIndex * _cPageSize);
            var page = _decodedEncodingFile.AsSpan(pageOffset, _cPageSize);
            if (TryResolveFromPage(page, targetCKey, out ekey))
                return true;
        }

        ekey = default;
        return false;
    }

    private static bool TryResolveFromPage(ReadOnlySpan<byte> page, CascKey targetCKey, out CascKey ekey)
    {
        int offset = 0;
        while (offset < page.Length)
        {
            // Fast padding detection.
            if (page[offset] == 0)
            {
                if (IsAllZero(page[offset..]))
                    break;
                offset++;
                continue;
            }

            if (offset + 1 + 5 + CascKey.Length > page.Length)
                break;

            byte keyCount = page[offset];
            if (keyCount == 0)
                break;

            offset += 1;

            // file_size (uint40 BE)
            offset += 5;

            var ckey = new CascKey(page.Slice(offset, CascKey.Length));
            offset += CascKey.Length;

            int ekeysBytes = checked(CascKey.Length * keyCount);
            if (offset + ekeysBytes > page.Length)
                break;

            // First EKey is used as the canonical mapping.
            var resolved = new CascKey(page.Slice(offset, CascKey.Length));
            offset += ekeysBytes;

            if (ckey == targetCKey)
            {
                ekey = resolved;
                return true;
            }
        }

        ekey = default;
        return false;
    }

    private static bool IsAllZero(ReadOnlySpan<byte> span)
    {
        for (int i = 0; i < span.Length; i++)
            if (span[i] != 0)
                return false;
        return true;
    }
}
