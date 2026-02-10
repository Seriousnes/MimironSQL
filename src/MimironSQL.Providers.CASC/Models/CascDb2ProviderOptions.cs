using Microsoft.Extensions.Configuration;

namespace MimironSQL.Providers;

/// <summary>
/// Options for configuring the CASC provider.
/// </summary>
public sealed record CascDb2ProviderOptions
{
    /// <summary>
    /// Root directory of the World of Warcraft installation.
    /// </summary>
    public string WowInstallRoot { get; init; } = string.Empty;

    /// <summary>
    /// Directory containing WoWDBDefs <c>.dbd</c> files.
    /// This is used by EF Core integrations that also register a file-system DBD provider.
    /// </summary>
    public string? DbdDefinitionsDirectory { get; init; }

    /// <summary>
    /// Directory where the DB2 manifest (e.g. <c>manifest.json</c>) is cached.
    /// If not specified, an implementation-defined default is used.
    /// </summary>
    public string? ManifestCacheDirectory { get; init; }

    /// <summary>
    /// Name of the manifest asset file (default: <c>manifest.json</c>).
    /// </summary>
    public string ManifestAssetName { get; init; } = "manifest.json";

    /// <summary>
    /// Binds <see cref="CascDb2ProviderOptions"/> from configuration.
    /// </summary>
    /// <remarks>
    /// This method intentionally avoids ConfigurationBinder dependencies and binds from a small set of keys.
    /// It reads from the <c>Casc</c> section first, then falls back to root-level keys for backwards compatibility.
    /// </remarks>
    /// <param name="configuration">The configuration root.</param>
    /// <returns>Bound options.</returns>
    public static CascDb2ProviderOptions FromConfiguration(IConfiguration configuration)
    {
        ArgumentNullException.ThrowIfNull(configuration);

        var casc = configuration.GetSection("Casc");

        static string? ReadString(IConfigurationSection section, IConfiguration root, string key)
            => section[key]?.Trim() is { Length: > 0 } v ? v : (root[key]?.Trim() is { Length: > 0 } r ? r : null);

        var wowInstallRoot = ReadString(casc, configuration, "WowInstallRoot") ?? string.Empty;
        var dbdDefsDir = ReadString(casc, configuration, "DbdDefinitionsDirectory");
        var cacheDir = ReadString(casc, configuration, "ManifestCacheDirectory");

        var assetName = casc["ManifestAssetName"]?.Trim();
        if (string.IsNullOrWhiteSpace(assetName))
            assetName = configuration["ManifestAssetName"]?.Trim();
        if (string.IsNullOrWhiteSpace(assetName))
            assetName = "manifest.json";

        return new CascDb2ProviderOptions
        {
            WowInstallRoot = wowInstallRoot,
            DbdDefinitionsDirectory = dbdDefsDir,
            ManifestCacheDirectory = cacheDir,
            ManifestAssetName = assetName,
        };
    }
}
