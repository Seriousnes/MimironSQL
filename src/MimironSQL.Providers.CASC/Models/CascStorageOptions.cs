namespace MimironSQL.Providers;

public sealed record CascStorageOptions
{
    /// <summary>
    /// When true, opening an install root through the DI service will ensure the configured
    /// <see cref="IManifestProvider"/> has cached data available (download as needed).
    /// </summary>
    public bool EnsureManifestOnOpenInstallRoot { get; init; } = true;
}
