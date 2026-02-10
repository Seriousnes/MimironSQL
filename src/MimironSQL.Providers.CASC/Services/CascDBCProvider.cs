namespace MimironSQL.Providers;

/// <summary>
/// Opens DB2 streams from CASC storage by resolving FileDataIds using a manifest provider.
/// </summary>
/// <param name="storage">The opened CASC storage.</param>
/// <param name="manifestProvider">The manifest provider used to resolve table names.</param>
internal sealed class CascDBCProvider(ICascStorage storage, IManifestProvider manifestProvider) : IDb2StreamProvider
{
    private readonly ICascStorage _storage = storage ?? throw new ArgumentNullException(nameof(storage));
    private readonly IManifestProvider _manifestProvider = manifestProvider ?? throw new ArgumentNullException(nameof(manifestProvider));

    /// <summary>
    /// Opens a DB2 stream for the specified table.
    /// </summary>
    /// <param name="tableName">The table name.</param>
    /// <returns>A readable stream for the DB2 file.</returns>
    public Stream OpenDb2Stream(string tableName)
    {
        ArgumentNullException.ThrowIfNull(tableName);

        var trimmed = tableName.Trim();
        var resolved = _manifestProvider.TryResolveDb2FileDataIdAsync(trimmed).GetAwaiter().GetResult();
        if (resolved is not { } fdid)
            throw new FileNotFoundException($"No .db2 file found for table '{trimmed}'.");

        return _storage.OpenDb2ByFileDataIdAsync(fdid).GetAwaiter().GetResult();
    }
}
