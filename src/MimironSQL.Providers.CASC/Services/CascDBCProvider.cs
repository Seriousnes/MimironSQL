namespace MimironSQL.Providers;

public sealed class CascDBCProvider(ICascStorage storage, IManifestProvider manifestProvider) : IDb2StreamProvider
{
    private readonly ICascStorage _storage = storage ?? throw new ArgumentNullException(nameof(storage));
    private readonly IManifestProvider _manifestProvider = manifestProvider ?? throw new ArgumentNullException(nameof(manifestProvider));

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
