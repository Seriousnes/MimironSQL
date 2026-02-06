using System.ComponentModel.DataAnnotations;

namespace MimironSQL.Providers;

public sealed record WowListfileOptions
{
    public string Owner { get; init; } = "wowdev";

    public string Repository { get; init; } = "wow-listfile";

    public string AssetName { get; init; } = "verified-listfile.csv";

    /// <summary>
    /// Directory where the listfile and metadata are cached.
    /// Defaults to %LOCALAPPDATA%\CASC.Net\wow-listfile.
    /// </summary>
    public string? CacheDirectory { get; init; }

    public bool DownloadOnStartup { get; init; } = false;

    [Range(1, int.MaxValue)]
    public int HttpTimeoutSeconds { get; init; } = 60;

    internal string GetCacheDirectoryOrDefault()
    {
        if (!string.IsNullOrWhiteSpace(CacheDirectory))
            return CacheDirectory;

        var localAppData = Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData);
        return Path.Combine(localAppData, "CASC.Net", "wow-listfile");
    }
}
