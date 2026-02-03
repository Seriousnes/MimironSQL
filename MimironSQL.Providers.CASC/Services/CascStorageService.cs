using Microsoft.Extensions.Options;

namespace MimironSQL.Providers;

public sealed class CascStorageService(IManifestProvider manifestProvider, IOptions<CascStorageOptions> storageOptions) : ICascStorageService
{
    private readonly IManifestProvider _manifestProvider = manifestProvider ?? throw new ArgumentNullException(nameof(manifestProvider));
    private readonly CascStorageOptions _storageOptions = storageOptions?.Value ?? throw new ArgumentNullException(nameof(storageOptions));

    public async Task<CascStorage> OpenInstallRootAsync(string installRoot, CancellationToken cancellationToken = default)
    {
        if (_storageOptions.EnsureManifestOnOpenInstallRoot)
            await _manifestProvider.EnsureManifestExistsAsync(cancellationToken).ConfigureAwait(false);

        return await CascStorage.OpenInstallRootAsync(installRoot, cancellationToken).ConfigureAwait(false);
    }
}
