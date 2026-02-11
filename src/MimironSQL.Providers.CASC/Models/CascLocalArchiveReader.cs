using System.Buffers;
using System.Text.RegularExpressions;

namespace MimironSQL.Providers;

internal sealed partial class CascLocalArchiveReader
{
    [GeneratedRegex("^(?<bucket>[0-9a-fA-F]{2})(?<version>[0-9a-fA-F]{8})$", RegexOptions.Compiled)]
    private static partial Regex GetIdxNameRegex();
    private static readonly Regex IdxNameRegex = GetIdxNameRegex();

    private static readonly System.Collections.Concurrent.ConcurrentDictionary<string, Lazy<CascIdxFile>> IdxByPathCache = new(StringComparer.OrdinalIgnoreCase);

    private readonly string _dataDataDirectory;
    private readonly Dictionary<byte, string> _activeIdxPathByBucket;

    public CascLocalArchiveReader(string dataDataDirectory)
    {
        ArgumentNullException.ThrowIfNull(dataDataDirectory);
        if (!Directory.Exists(dataDataDirectory))
            throw new DirectoryNotFoundException(dataDataDirectory);

        _dataDataDirectory = dataDataDirectory;
        _activeIdxPathByBucket = SelectActiveIdxPathsByBucket(dataDataDirectory);
    }

    public bool TryGetEncodedLocation(CascKey ekey, out CascIdxEntry entry)
    {
        Span<byte> keyBytes = stackalloc byte[CascKey.Length];
        ekey.CopyTo(keyBytes);

        var bucket = CascBucket.GetBucketIndex(keyBytes);
        if (TryGetIdx(bucket) is { } idx && TryFindEntry(idx, keyBytes, out entry))
            return true;

        var crossBucket = CascBucket.GetBucketIndexCrossReference(keyBytes);
        if (TryGetIdx(crossBucket) is { } crossIdx && TryFindEntry(crossIdx, keyBytes, out entry))
            return true;

        entry = default;
        return false;
    }

    private CascIdxFile? TryGetIdx(byte bucket)
    {
        if (!_activeIdxPathByBucket.TryGetValue(bucket, out var path))
            return null;

        var lazy = IdxByPathCache.GetOrAdd(path, static p => new Lazy<CascIdxFile>(() => ReadIdx(p)));
        return lazy.Value;

        static CascIdxFile ReadIdx(string path)
        {
            using var fs = File.OpenRead(path);
            return CascIdxFile.Read(fs);
        }
    }

    public async Task<byte[]> ReadBlteBytesAsync(CascKey ekey, CancellationToken cancellationToken = default)
    {
        if (!TryGetEncodedLocation(ekey, out var entry))
            throw new KeyNotFoundException($"EKey not found in local .idx journals: {ekey}");

        var dataPath = Path.Combine(_dataDataDirectory, $"data.{entry.ArchiveIndex:D3}");
        if (!File.Exists(dataPath))
            throw new FileNotFoundException("CASC data archive not found", dataPath);

        await using var fs = new FileStream(
            dataPath,
            FileMode.Open,
            FileAccess.Read,
            FileShare.Read,
            bufferSize: 1024 * 64,
            options: FileOptions.Asynchronous | FileOptions.RandomAccess);

        fs.Seek(entry.Offset, SeekOrigin.Begin);

        // Some installs appear to store .idx offsets pointing directly to BLTE,
        // while others point to a small record header followed by BLTE.
        // We detect by checking for the BLTE signature at the offset.
        var sig = new byte[4];
        await ReadExactlyAsync(fs, sig, cancellationToken).ConfigureAwait(false);

        int blteSize = checked((int)entry.Size);
        if (blteSize <= 0)
            throw new InvalidDataException("Invalid encoded size.");

        bool isBlteAtOffset = sig[0] == (byte)'B' && sig[1] == (byte)'L' && sig[2] == (byte)'T' && sig[3] == (byte)'E';
        if (isBlteAtOffset)
        {
            fs.Seek(entry.Offset, SeekOrigin.Begin);
            var blte = new byte[blteSize];
            await ReadExactlyAsync(fs, blte, cancellationToken).ConfigureAwait(false);
            return blte;
        }

        // Fallback: many installs point .idx offsets at a small record header followed by BLTE.
        // In that case, entry.Size refers to the total record size at entry.Offset (header + BLTE).
        // BLTE commonly starts at +0x1E, but do not hardcode; scan a small prefix.
        // This avoids allocating a full record buffer and then copying the BLTE tail.
        int prefixLen = Math.Min(blteSize, 0x80 + 4);
        byte[] rented = ArrayPool<byte>.Shared.Rent(prefixLen);
        try
        {
            fs.Seek(entry.Offset, SeekOrigin.Begin);
            await ReadExactlyAsync(fs, rented.AsMemory(0, prefixLen), cancellationToken).ConfigureAwait(false);

            int scanLimit = prefixLen - 4;
            int blteOffset = -1;
            for (int i = 0; i <= scanLimit; i++)
            {
                if (rented[i + 0] == (byte)'B' && rented[i + 1] == (byte)'L' && rented[i + 2] == (byte)'T' && rented[i + 3] == (byte)'E')
                {
                    blteOffset = i;
                    break;
                }
            }

            if (blteOffset < 0)
                throw new InvalidDataException("Unable to locate BLTE signature within archive record.");

            int blteLen = blteSize - blteOffset;
            if (blteLen <= 0)
                throw new InvalidDataException("Invalid BLTE length.");

            var blteBytes = new byte[blteLen];
            fs.Seek(entry.Offset + blteOffset, SeekOrigin.Begin);
            await ReadExactlyAsync(fs, blteBytes, cancellationToken).ConfigureAwait(false);
            return blteBytes;
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(rented);
        }
    }

    private static bool TryFindEntry(CascIdxFile idx, ReadOnlySpan<byte> ekeyBytes, out CascIdxEntry entry)
    {
        var keyLen = idx.Header.Spec.Key;
        if (keyLen <= 0 || keyLen > CascKey.Length)
            throw new InvalidDataException("Unsupported idx key prefix length.");

        var prefix = ekeyBytes[..keyLen];
        var entries = idx.Entries;

        int lo = 0;
        int hi = entries.Count - 1;
        while (lo <= hi)
        {
            int mid = lo + ((hi - lo) / 2);
            var midPrefix = entries[mid].KeyPrefix.Span;
            var cmp = Compare(prefix, midPrefix);
            if (cmp == 0)
            {
                entry = entries[mid];
                return true;
            }

            if (cmp < 0)
                hi = mid - 1;
            else
                lo = mid + 1;
        }

        entry = default;
        return false;
    }

    private static int Compare(ReadOnlySpan<byte> a, ReadOnlySpan<byte> b)
    {
        int len = Math.Min(a.Length, b.Length);
        for (int i = 0; i < len; i++)
        {
            int diff = a[i] - b[i];
            if (diff != 0)
                return diff;
        }
        return a.Length - b.Length;
    }

    private static Dictionary<byte, string> SelectActiveIdxPathsByBucket(string dataDataDirectory)
    {
        var idxPaths = Directory.EnumerateFiles(dataDataDirectory, "*.idx", SearchOption.TopDirectoryOnly);

        var shmemVersions = TryReadShmemVersions(Path.Combine(dataDataDirectory, "shmem"));

        var candidatesByBucket = new Dictionary<byte, List<(uint? nameVersion, string path)>>();
        foreach (var path in idxPaths)
        {
            var fileName = Path.GetFileNameWithoutExtension(path);
            if (fileName.Length != 10)
                continue;

            var m = IdxNameRegex.Match(fileName);
            if (!m.Success)
                continue;

            if (!byte.TryParse(m.Groups["bucket"].Value, System.Globalization.NumberStyles.HexNumber, null, out var bucket))
                continue;

            uint? version = uint.TryParse(m.Groups["version"].Value, System.Globalization.NumberStyles.HexNumber, null, out var v)
                ? v
                : null;

            if (!candidatesByBucket.TryGetValue(bucket, out var list))
            {
                list = [];
                candidatesByBucket[bucket] = list;
            }

            list.Add((version, path));
        }

        var chosenPaths = new Dictionary<byte, string>();
        for (byte bucket = 0; bucket < 16; bucket++)
        {
            if (!candidatesByBucket.TryGetValue(bucket, out var list) || list.Count == 0)
                continue;

            if (shmemVersions is not null && shmemVersions.Length == 16)
            {
                var desired = shmemVersions[bucket];
                var (nameVersion, path) = list.FirstOrDefault(x => x.nameVersion == desired);
                if (path is not null)
                {
                    chosenPaths[bucket] = path;
                    continue;
                }
            }

            // Fallback: choose max version by filename, else first.
            var best = list
                .OrderByDescending(x => x.nameVersion ?? 0)
                .First();

            chosenPaths[bucket] = best.path;
        }

        return chosenPaths;
    }

    private static uint[]? TryReadShmemVersions(string shmemPath)
    {
        if (!File.Exists(shmemPath))
            return null;

        CascShmemFile shmem;
        try
        {
            using var fs = new FileStream(shmemPath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite | FileShare.Delete);
            shmem = CascShmemFile.Read(fs);
        }
        catch (IOException)
        {
            return null;
        }

        if (shmem.IdxVersions.Count != 16)
            return null;

        var versions = new uint[16];
        for (int i = 0; i < versions.Length; i++)
            versions[i] = shmem.IdxVersions[i];

        return versions;
    }

    private static async Task ReadExactlyAsync(Stream stream, Memory<byte> buffer, CancellationToken cancellationToken)
    {
        await stream.ReadExactlyAsync(buffer, cancellationToken).ConfigureAwait(false);
    }
}
