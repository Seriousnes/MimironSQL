namespace MimironSQL.Providers;

/// <summary>
/// DI-friendly service for opening <see cref="ICascStorage"/> instances.
/// </summary>
/// <param name="manifestProvider">The manifest provider used for optional initialization.</param>
internal sealed class CascStorageService(IManifestProvider manifestProvider) : ICascStorageService
{
    private readonly IManifestProvider _manifestProvider = manifestProvider ?? throw new ArgumentNullException(nameof(manifestProvider));

    /// <summary>
    /// Opens CASC storage for the specified install root.
    /// </summary>
    /// <param name="installRoot">Root directory of the World of Warcraft installation.</param>
    /// <param name="cancellationToken">A cancellation token.</param>
    /// <returns>An opened CASC storage instance.</returns>
    public async Task<ICascStorage> OpenInstallRootAsync(string installRoot, CancellationToken cancellationToken = default)
    {
        await _manifestProvider.EnsureManifestExistsAsync(cancellationToken).ConfigureAwait(false);

        return await CascStorage.OpenInstallRootAsync(installRoot, cancellationToken).ConfigureAwait(false);
    }
}
