namespace MimironSQL.IntegrationTests.Helpers;

internal static class LocalEnvLocal
{
    public static bool TryGetWowInstallRoot(out string wowInstallRoot)
    {
        wowInstallRoot = string.Empty;

        var path = GetEnvLocalPath();
        if (!File.Exists(path))
            return false;

        foreach (var line in File.ReadLines(path))
        {
            var trimmed = line.Trim();
            if (trimmed.Length == 0)
                continue;

            if (trimmed.StartsWith('#'))
                continue;

            var equals = trimmed.IndexOf('=');
            if (equals <= 0)
                continue;

            var key = trimmed[..equals].Trim();
            if (!string.Equals(key, "WOW_INSTALL_ROOT", StringComparison.OrdinalIgnoreCase))
                continue;

            var value = trimmed[(equals + 1)..].Trim();
            wowInstallRoot = TrimOptionalQuotes(value);

            return wowInstallRoot.Length > 0;
        }

        return false;
    }

    public static string GetEnvLocalPath()
    {
        var baseDir = AppContext.BaseDirectory;
        return System.IO.Path.GetFullPath(System.IO.Path.Combine(baseDir, "..", "..", "..", ".env.local"));
    }

    private static string TrimOptionalQuotes(string value)
    {
        if (value is ['"', .., '"'] || value is ['\'', .., '\''])
            return value[1..^1].Trim();

        return value;
    }
}