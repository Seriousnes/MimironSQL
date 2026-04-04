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
    private readonly ConcurrentDictionary<CascKey, CascKey[]> _resolvedAll = new();

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
        // Header is 0x16 (22) bytes:
        // EN (2) + version (1) + ckey_size (1) + ekey_size (1)
        // + c_page_size_kb (2) + e_page_size_kb (2)
        // + c_page_count (4) + e_page_count (4)
        // + unk1 (1) + e_spec_block_size (4)
        if (decodedEncodingFile.Length < 0x16)
        {
            throw new InvalidDataException("ENCODING file too small");
        }

        if (decodedEncodingFile[0] != (byte)'E' || decodedEncodingFile[1] != (byte)'N')
        {
            throw new InvalidDataException("ENCODING signature not found");
        }

        byte version = decodedEncodingFile[2];
        if (version != 1)
        {
            throw new InvalidDataException($"Unsupported ENCODING version: {version}");
        }

        int ckeySize = decodedEncodingFile[3];
        int ekeySize = decodedEncodingFile[4];
        if (ckeySize != CascKey.Length || ekeySize != CascKey.Length)
        {
            throw new InvalidDataException($"Unsupported key sizes (ckey={ckeySize}, ekey={ekeySize})");
        }

        int cPageSize = checked(BinaryPrimitives.ReadUInt16BigEndian(decodedEncodingFile.AsSpan(5, 2)) * 1024);
        _ = checked(BinaryPrimitives.ReadUInt16BigEndian(decodedEncodingFile.AsSpan(7, 2)) * 1024); // e_page_size_kb (currently unused)

        int cPageCount = checked((int)BinaryPrimitives.ReadUInt32BigEndian(decodedEncodingFile.AsSpan(9, 4)));
        _ = checked((int)BinaryPrimitives.ReadUInt32BigEndian(decodedEncodingFile.AsSpan(13, 4))); // e_page_count (currently unused)

        byte unk1 = decodedEncodingFile[17];
        if (unk1 != 0)
        {
            // Not expected, but tolerate unknown values for now.
        }

        int eSpecSize = checked((int)BinaryPrimitives.ReadUInt32BigEndian(decodedEncodingFile.AsSpan(18, 4)));

        int offset = 0x16;
        if (offset + eSpecSize > decodedEncodingFile.Length)
        {
            throw new InvalidDataException("ENCODING ESpec block exceeds file size");
        }

        offset += eSpecSize;

        // CEKeyPageTable
        int cIndexEntrySize = ckeySize + 16;
        int cIndexSize = checked(cPageCount * cIndexEntrySize);
        int cPagesSize = checked(cPageCount * cPageSize);

        if (offset + cIndexSize > decodedEncodingFile.Length)
        {
            throw new InvalidDataException("ENCODING CEKey page index exceeds file size");
        }

        offset += cIndexSize;

        if (offset + cPagesSize > decodedEncodingFile.Length)
        {
            throw new InvalidDataException("ENCODING CEKey pages exceed file size");
        }

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
        {
            return true;
        }

        if (!TryGetEKeys(ckey, out var ekeys))
        {
            ekey = default;
            return false;
        }

        ekey = ekeys[0];
        _resolved.TryAdd(ckey, ekey);
        return true;
    }

    /// <summary>
    /// Attempts to resolve all EKeys for the provided CKey.
    /// </summary>
    public bool TryGetEKeys(CascKey ckey, out CascKey[] ekeys)
    {
        if (_resolvedAll.TryGetValue(ckey, out ekeys!))
        {
            return true;
        }

        if (!TryResolveAllFromPages(ckey, out ekeys))
        {
            ekeys = [];
            return false;
        }

        _resolvedAll.TryAdd(ckey, ekeys);
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
        {
            throw new KeyNotFoundException($"CKey not found in ENCODING: {ckey}");
        }

        return ekey;
    }

    private bool TryResolveAllFromPages(CascKey targetCKey, out CascKey[] ekeys)
    {
        // Simple, low-memory strategy: scan CKey pages until we find a match.
        // In practice we only resolve a relatively small number of keys per process.
        for (int pageIndex = 0; pageIndex < _cPageCount; pageIndex++)
        {
            var pageOffset = _cPagesOffset + (pageIndex * _cPageSize);
            var page = _decodedEncodingFile.AsSpan(pageOffset, _cPageSize);
            if (TryResolveAllFromPage(page, targetCKey, out ekeys))
            {
                return true;
            }
        }

        ekeys = [];
        return false;
    }

    private static bool TryResolveAllFromPage(ReadOnlySpan<byte> page, CascKey targetCKey, out CascKey[] ekeys)
    {
        int offset = 0;
        while (offset < page.Length)
        {
            // Fast padding detection.
            if (page[offset] == 0)
            {
                if (IsAllZero(page[offset..]))
                {
                    break;
                }

                offset++;
                continue;
            }

            if (offset + 1 + 5 + CascKey.Length > page.Length)
            {
                break;
            }

            byte keyCount = page[offset];
            if (keyCount == 0)
            {
                break;
            }

            offset += 1;

            // file_size (uint40 BE)
            offset += 5;

            var ckey = new CascKey(page.Slice(offset, CascKey.Length));
            offset += CascKey.Length;

            int ekeysBytes = checked(CascKey.Length * keyCount);
            if (offset + ekeysBytes > page.Length)
            {
                break;
            }

            if (ckey == targetCKey)
            {
                ekeys = new CascKey[keyCount];
                for (int i = 0; i < keyCount; i++)
                {
                    ekeys[i] = new CascKey(page.Slice(offset + (i * CascKey.Length), CascKey.Length));
                }

                return true;
            }

            offset += ekeysBytes;
        }

        ekeys = [];
        return false;
    }

    private static bool IsAllZero(ReadOnlySpan<byte> span)
    {
        for (int i = 0; i < span.Length; i++)
        {
            if (span[i] != 0)
            {
                return false;
            }
        }

        return true;
    }
}
