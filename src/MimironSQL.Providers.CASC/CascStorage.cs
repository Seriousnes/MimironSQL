namespace MimironSQL.Providers;

public sealed class CascStorage : ICascStorage
{
    private readonly CascLocalArchiveReader? _archiveReader;
    private readonly CascEncodingIndex? _encoding;
    private readonly CascRootIndex? _root;

    public event Action<CascStorageEncryptedBlteBlocksSkipped>? EncryptedBlteBlocksSkipped;

    internal int RootEntryCount => _root?.EntryCount ?? 0;

    internal bool HasFileDataId(int fileDataId) => _root is not null && _root.TryGetContentKey(fileDataId, out _);

    internal bool TryResolveFileDataIdDebug(
        int fileDataId,
        out CascKey contentKey,
        out uint contentFlags,
        out uint localeFlags,
        out CascKey resolvedEKey,
        out bool usedEncoding,
        out bool hasArchiveLocation)
    {
        contentKey = default;
        contentFlags = 0;
        localeFlags = 0;
        resolvedEKey = default;
        usedEncoding = false;
        hasArchiveLocation = false;

        if (_root is null)
            return false;
        if (_archiveReader is null)
            return false;

        if (!_root.TryGetDebug(fileDataId, out contentKey, out contentFlags, out localeFlags))
            return false;

        if (_encoding is not null && _encoding.TryGetEKey(contentKey, out var ekey))
        {
            resolvedEKey = ekey;
            usedEncoding = true;
        }
        else
        {
            // Fallback: sometimes the value is already an EKey.
            resolvedEKey = contentKey;
            usedEncoding = false;
        }

        hasArchiveLocation = _archiveReader.TryGetEncodedLocation(resolvedEKey, out _);
        return true;
    }

    private CascStorage(
        CascLocalArchiveReader? archiveReader = null,
        CascEncodingIndex? encoding = null,
        CascRootIndex? root = null)
    {
        _archiveReader = archiveReader;
        _encoding = encoding;
        _root = root;
    }

    public static async Task<CascStorage> OpenInstallRootAsync(string installRoot, CancellationToken cancellationToken = default)
    {
        var layout = CascInstallLayoutDetector.Detect(installRoot);

        var buildInfo = CascBuildInfo.Read(layout.BuildInfoPath);
        var record = CascBuildInfo.SelectForProduct(buildInfo, layout.Product);

        var buildConfigKey = CascKey.ParseHex(record.BuildConfig);
        var buildConfigBytes = CascConfigStore.ReadConfigBytes(layout.DataConfigDirectory, buildConfigKey);
        var buildConfig = CascBuildConfigParser.Read(buildConfigBytes);

        var archiveReader = new CascLocalArchiveReader(layout.DataDataDirectory);

        if (buildConfig.EncodingEKey is not { } encodingEKey)
            throw new NotSupportedException("Build config did not include an ENCODING EKey; local-only resolution is not implemented for this case.");

        var encodingBlte = await archiveReader.ReadBlteBytesAsync(encodingEKey, cancellationToken).ConfigureAwait(false);
        var encodingDecoded = BlteDecoder.Decode(encodingBlte);
        var encoding = CascEncodingIndex.Parse(encodingDecoded);

        var rootEKey = buildConfig.RootEKey ?? encoding.GetEKey(buildConfig.RootCKey);
        var rootBlte = await archiveReader.ReadBlteBytesAsync(rootEKey, cancellationToken).ConfigureAwait(false);
        var rootDecoded = BlteDecoder.Decode(rootBlte);
        var root = CascRootIndex.Parse(rootDecoded);

        return new CascStorage(archiveReader: archiveReader, encoding: encoding, root: root);
    }

    public async Task<Stream?> TryOpenDb2ByCKeyAsync(CascKey ckey, CancellationToken cancellationToken = default)
    {
        if (_archiveReader is null || _encoding is null)
            throw new InvalidOperationException("Storage was not opened from an install root.");

        if (_encoding.TryGetEKey(ckey, out var resolvedEKey))
            return await TryOpenDb2ByEKeyAsync(resolvedEKey, cancellationToken).ConfigureAwait(false);

        // Fallback: sometimes hashes are already EKeys.
        return await TryOpenDb2ByEKeyAsync(ckey, cancellationToken).ConfigureAwait(false);
    }

    public async Task<Stream> OpenDb2ByCKeyAsync(CascKey ckey, CancellationToken cancellationToken = default)
        => await TryOpenDb2ByCKeyAsync(ckey, cancellationToken).ConfigureAwait(false)
           ?? throw new FileNotFoundException($"DB2 file not found by CKey: {ckey}");

    public async Task<Stream?> TryOpenDb2ByEKeyAsync(CascKey ekey, CancellationToken cancellationToken = default)
    {
        if (_archiveReader is null)
            throw new InvalidOperationException("Storage was not opened from an install root.");

        try
        {
            var blte = await _archiveReader.ReadBlteBytesAsync(ekey, cancellationToken).ConfigureAwait(false);
            int skippedBlockCount = 0;
            long skippedLogicalBytes = 0;

            var decoded = BlteDecoder.Decode(blte, new BlteDecodeOptions(skipped =>
            {
                skippedBlockCount++;
                skippedLogicalBytes += skipped.LogicalSize;
            }));

            if (skippedBlockCount > 0)
                EncryptedBlteBlocksSkipped?.Invoke(new CascStorageEncryptedBlteBlocksSkipped(ekey, skippedBlockCount, skippedLogicalBytes));

            return new MemoryStream(decoded, writable: false);
        }
        catch (KeyNotFoundException)
        {
            // EKey not present in local .idx journals.
            return null;
        }
    }

    public async Task<Stream> OpenDb2ByEKeyAsync(CascKey ekey, CancellationToken cancellationToken = default)
        => await TryOpenDb2ByEKeyAsync(ekey, cancellationToken).ConfigureAwait(false)
           ?? throw new FileNotFoundException($"DB2 file not found by EKey: {ekey}");

    public async Task<Stream?> TryOpenDb2ByFileDataIdAsync(int fileDataId, CancellationToken cancellationToken = default)
    {
        if (_root is null)
            throw new InvalidOperationException("ROOT index was not loaded.");

        if (!_root.TryGetContentKey(fileDataId, out var ckey))
            return null;

        return await TryOpenDb2ByCKeyAsync(ckey, cancellationToken).ConfigureAwait(false);
    }

    public async Task<Stream> OpenDb2ByFileDataIdAsync(int fileDataId, CancellationToken cancellationToken = default)
        => await TryOpenDb2ByFileDataIdAsync(fileDataId, cancellationToken).ConfigureAwait(false)
           ?? throw new FileNotFoundException($"DB2 not found by FileDataId: {fileDataId}");
}
