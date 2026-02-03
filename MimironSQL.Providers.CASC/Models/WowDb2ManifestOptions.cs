using System.ComponentModel.DataAnnotations;

namespace MimironSQL.Providers;

public sealed record WowDb2ManifestOptions
{
    public string Owner { get; init; } = "wowdev";

    public string Repository { get; init; } = "WoWDBDefs";

    public string AssetName { get; init; } = "manifest.json";

    /// <summary>
    /// Directory where the manifest and metadata are cached.
    /// When present, a local <see cref="AssetName"/> in this directory is preferred over downloading.
    /// Defaults to %LOCALAPPDATA%\CASC.Net\wowdbdefs.
    /// </summary>
    public string? CacheDirectory { get; init; }

    [Range(1, int.MaxValue)]
    public int HttpTimeoutSeconds { get; init; } = 60;

    public string GetCacheDirectoryOrDefault()
    {
        if (!string.IsNullOrWhiteSpace(CacheDirectory))
            return CacheDirectory;

        var localAppData = Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData);
        return Path.Combine(localAppData, "CASC.Net", "wowdbdefs");
    }
}
