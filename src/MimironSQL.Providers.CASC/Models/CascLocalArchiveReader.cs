using System.Text.RegularExpressions;

namespace MimironSQL.Providers;

internal sealed class CascLocalArchiveReader
{
    private static readonly Regex IdxNameRegex = new("^(?<bucket>[0-9a-fA-F]{2})(?<version>[0-9a-fA-F]{8})$", RegexOptions.Compiled);

    private readonly string _dataDataDirectory;
    private readonly IReadOnlyDictionary<byte, CascIdxFile> _activeIdxByBucket;

    public CascLocalArchiveReader(string dataDataDirectory)
    {
        ArgumentNullException.ThrowIfNull(dataDataDirectory);
        if (!Directory.Exists(dataDataDirectory))
            throw new DirectoryNotFoundException(dataDataDirectory);

        _dataDataDirectory = dataDataDirectory;
        _activeIdxByBucket = LoadActiveIdxByBucket(dataDataDirectory);
    }

    public bool TryGetEncodedLocation(CascKey ekey, out CascIdxEntry entry)
    {
        Span<byte> keyBytes = stackalloc byte[CascKey.Length];
        ekey.CopyTo(keyBytes);

        var bucket = CascBucket.GetBucketIndex(keyBytes);
        if (_activeIdxByBucket.TryGetValue(bucket, out var idx) && TryFindEntry(idx, keyBytes, out entry))
            return true;

        var crossBucket = CascBucket.GetBucketIndexCrossReference(keyBytes);
        if (_activeIdxByBucket.TryGetValue(crossBucket, out var crossIdx) && TryFindEntry(crossIdx, keyBytes, out entry))
            return true;

        entry = default;
        return false;
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
        fs.Seek(entry.Offset, SeekOrigin.Begin);
        var record = new byte[blteSize];
        await ReadExactlyAsync(fs, record, cancellationToken).ConfigureAwait(false);

        // BLTE commonly starts at +0x1E, but do not hardcode; scan a small prefix.
        int scanLimit = Math.Min(record.Length - 4, 0x80);
        int blteOffset = -1;
        for (int i = 0; i <= scanLimit; i++)
        {
            if (record[i + 0] == (byte)'B' && record[i + 1] == (byte)'L' && record[i + 2] == (byte)'T' && record[i + 3] == (byte)'E')
            {
                blteOffset = i;
                break;
            }
        }

        if (blteOffset < 0)
            throw new InvalidDataException("Unable to locate BLTE signature within archive record.");

        var blteBytes = new byte[record.Length - blteOffset];
        Buffer.BlockCopy(record, blteOffset, blteBytes, 0, blteBytes.Length);
        return blteBytes;
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

    private static IReadOnlyDictionary<byte, CascIdxFile> LoadActiveIdxByBucket(string dataDataDirectory)
    {
        var idxPaths = Directory.EnumerateFiles(dataDataDirectory, "*.idx", SearchOption.TopDirectoryOnly);

        var shmemVersions = TryReadShmemVersions(Path.Combine(dataDataDirectory, "shmem"));

        var candidatesByBucket = new Dictionary<byte, List<(uint? nameVersion, string path)>>() ;
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

        var idxByBucket = new Dictionary<byte, CascIdxFile>();
        foreach (var (bucket, path) in chosenPaths)
        {
            using var fs = File.OpenRead(path);
            var idx = CascIdxFile.Read(fs);
            idxByBucket[bucket] = idx;
        }

        return idxByBucket;
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

    private static async Task ReadExactlyAsync(Stream stream, byte[] buffer, CancellationToken cancellationToken)
    {
        int readTotal = 0;
        while (readTotal < buffer.Length)
        {
            int read = await stream.ReadAsync(buffer.AsMemory(readTotal), cancellationToken).ConfigureAwait(false);
            if (read <= 0)
                throw new EndOfStreamException();
            readTotal += read;
        }
    }
}
