namespace MimironSQL.Providers;

/// <summary>
/// Provides read-only access to DB2 files from a World of Warcraft installation via CASC.
/// </summary>
/// <param name="manifestProvider">The manifest provider used to resolve table names to FileDataIds.</param>
/// <param name="options">CASC provider options.</param>
public sealed class CascStorageService(IManifestProvider manifestProvider, CascDb2ProviderOptions options) : IDb2StreamProvider
{
    private readonly IManifestProvider _manifestProvider = manifestProvider ?? throw new ArgumentNullException(nameof(manifestProvider));
    private readonly CascDb2ProviderOptions _options = options ?? throw new ArgumentNullException(nameof(options));

    private readonly SemaphoreSlim _initLock = new(1, 1);
    private bool _initialized;

    private CascLocalArchiveReader? _archiveReader;
    private CascEncodingIndex? _encoding;
    private CascRootIndex? _root;

    /// <summary>
    /// Raised when encrypted BLTE blocks are encountered and skipped during decoding.
    /// </summary>
    internal event Action<CascStorageEncryptedBlteBlocksSkipped>? EncryptedBlteBlocksSkipped;

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
            resolvedEKey = contentKey;
            usedEncoding = false;
        }

        hasArchiveLocation = _archiveReader.TryGetEncodedLocation(resolvedEKey, out _);
        return true;
    }

    /// <summary>
    /// Opens a readable stream for a DB2 table.
    /// </summary>
    /// <param name="tableName">The DB2 table name (for example, <c>Map</c>).</param>
    /// <returns>A readable stream positioned at the start of the DB2 file content.</returns>
    public Stream OpenDb2Stream(string tableName)
    {
        ArgumentNullException.ThrowIfNull(tableName);

        return OpenDb2StreamAsync(tableName, cancellationToken: default).ConfigureAwait(false).GetAwaiter().GetResult();
    }

    /// <summary>
    /// Opens a readable stream for a DB2 table asynchronously.
    /// </summary>
    /// <param name="tableName">The DB2 table name (for example, <c>Map</c>).</param>
    /// <param name="cancellationToken">A cancellation token.</param>
    /// <returns>A task that produces a readable stream positioned at the start of the DB2 file content.</returns>
    public async Task<Stream> OpenDb2StreamAsync(string tableName, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(tableName);

        var trimmed = tableName.Trim();
        var resolved = await _manifestProvider.TryResolveDb2FileDataIdAsync(trimmed, cancellationToken).ConfigureAwait(false);
        if (resolved is not { } fdid)
            throw new FileNotFoundException($"No .db2 file found for table '{trimmed}'.");

        return await OpenDb2ByFileDataIdAsync(fdid, cancellationToken).ConfigureAwait(false);
    }

    internal async Task<Stream?> TryOpenDb2ByCKeyAsync(CascKey ckey, CancellationToken cancellationToken = default)
    {
        await EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

        if (_archiveReader is null || _encoding is null)
            throw new InvalidOperationException("Storage was not opened from an install root.");

        if (_encoding.TryGetEKey(ckey, out var resolvedEKey))
            return await TryOpenDb2ByEKeyAsync(resolvedEKey, cancellationToken).ConfigureAwait(false);

        return await TryOpenDb2ByEKeyAsync(ckey, cancellationToken).ConfigureAwait(false);
    }

    internal async Task<Stream> OpenDb2ByCKeyAsync(CascKey ckey, CancellationToken cancellationToken = default)
        => await TryOpenDb2ByCKeyAsync(ckey, cancellationToken).ConfigureAwait(false)
           ?? throw new FileNotFoundException($"DB2 file not found by CKey: {ckey}");

    internal async Task<Stream?> TryOpenDb2ByEKeyAsync(CascKey ekey, CancellationToken cancellationToken = default)
    {
        await EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

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
            return null;
        }
    }

    internal async Task<Stream> OpenDb2ByEKeyAsync(CascKey ekey, CancellationToken cancellationToken = default)
        => await TryOpenDb2ByEKeyAsync(ekey, cancellationToken).ConfigureAwait(false)
           ?? throw new FileNotFoundException($"DB2 file not found by EKey: {ekey}");

    internal async Task<Stream?> TryOpenDb2ByFileDataIdAsync(int fileDataId, CancellationToken cancellationToken = default)
    {
        await EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

        if (_root is null)
            throw new InvalidOperationException("ROOT index was not loaded.");

        if (!_root.TryGetContentKey(fileDataId, out var ckey))
            return null;

        return await TryOpenDb2ByCKeyAsync(ckey, cancellationToken).ConfigureAwait(false);
    }

    internal async Task<Stream> OpenDb2ByFileDataIdAsync(int fileDataId, CancellationToken cancellationToken = default)
        => await TryOpenDb2ByFileDataIdAsync(fileDataId, cancellationToken).ConfigureAwait(false)
           ?? throw new FileNotFoundException($"DB2 not found by FileDataId: {fileDataId}");

    private async Task EnsureInitializedAsync(CancellationToken cancellationToken)
    {
        if (_initialized)
            return;

        await _initLock.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (_initialized)
                return;

            await _manifestProvider.EnsureManifestExistsAsync(cancellationToken).ConfigureAwait(false);

            var installRoot = _options.WowInstallRoot;
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

            _archiveReader = archiveReader;
            _encoding = encoding;
            _root = root;
            _initialized = true;
        }
        finally
        {
            _initLock.Release();
        }
    }
}
