namespace MimironSQL.Providers;

/// <summary>
/// Shared contract for providers that download a specific asset from a GitHub release and cache it locally.
/// </summary>
public interface IGitHubReleaseAssetProvider
{
    /// <summary>
    /// Ensures the configured asset is present in the local cache.
    /// Downloads it if missing or outdated.
    /// </summary>
    Task EnsureDownloadedAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Opens the cached asset for reading.
    /// </summary>
    Task<Stream> OpenCachedAsync(CancellationToken cancellationToken = default);
}
