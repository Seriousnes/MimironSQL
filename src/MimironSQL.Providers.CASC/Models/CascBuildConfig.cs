namespace MimironSQL.Providers;

/// <summary>
/// Represents the subset of CASC build configuration keys required by this provider.
/// </summary>
/// <param name="EncodingCKey">The ENCODING content key (CKey).</param>
/// <param name="EncodingEKey">The ENCODING encoded key (EKey), when present.</param>
/// <param name="RootCKey">The ROOT content key (CKey).</param>
/// <param name="RootEKey">The ROOT encoded key (EKey), when present.</param>
/// <param name="InstallCKey">The INSTALL content key (CKey).</param>
/// <param name="InstallEKey">The INSTALL encoded key (EKey), when present.</param>
internal sealed record CascBuildConfig(
    CascKey EncodingCKey,
    CascKey? EncodingEKey,
    CascKey RootCKey,
    CascKey? RootEKey,
    CascKey InstallCKey,
    CascKey? InstallEKey);

/// <summary>
/// Parses CASC build configuration files.
/// </summary>
internal static class CascBuildConfigParser
{
    /// <summary>
    /// Reads and parses a build config file from disk.
    /// </summary>
    /// <param name="buildConfigPath">Path to the build config file.</param>
    /// <returns>The parsed build configuration.</returns>
    public static CascBuildConfig ReadFromFile(string buildConfigPath)
    {
        ArgumentNullException.ThrowIfNull(buildConfigPath);
        if (!File.Exists(buildConfigPath))
            throw new FileNotFoundException("Build config file not found", buildConfigPath);

        var bytes = File.ReadAllBytes(buildConfigPath);
        return Read(bytes);
    }

    /// <summary>
    /// Parses build configuration from bytes.
    /// </summary>
    /// <param name="buildConfigBytes">Build config file bytes.</param>
    /// <returns>The parsed build configuration.</returns>
    public static CascBuildConfig Read(ReadOnlySpan<byte> buildConfigBytes)
    {
        if (buildConfigBytes.Length == 0)
            throw new InvalidDataException("Build config is empty");

        // Build config is plain text (ASCII/UTF-8).
        var text = System.Text.Encoding.UTF8.GetString(buildConfigBytes);
        return ReadFromText(text);
    }

    private static CascBuildConfig ReadFromText(string text)
    {
        var lines = text.Split('\n');
        var map = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

        foreach (var raw in lines)
        {
            var line = raw.Trim();
            if (line.Length == 0 || line.StartsWith('#'))
                continue;

            int eq = line.IndexOf('=');
            if (eq < 0)
                continue;

            var key = line[..eq].Trim();
            var value = line[(eq + 1)..].Trim();
            if (key.Length == 0 || value.Length == 0)
                continue;

            map[key] = value;
        }

        var (ckey, ekeyOptional) = ParseKeys(Require(map, "encoding"));
        var (rootCKey, rootEKeyOptional) = ParseKeys(Require(map, "root"));
        var (installCKey, installEKeyOptional) = ParseKeys(Require(map, "install"));

        return new CascBuildConfig(ckey, ekeyOptional, rootCKey, rootEKeyOptional, installCKey, installEKeyOptional);
    }

    private static string Require(Dictionary<string, string> map, string key)
    {
        if (!map.TryGetValue(key, out var value))
            throw new InvalidDataException($"Build config missing required key '{key}'.");
        return value;
    }

    private static (CascKey ckey, CascKey? ekeyOptional) ParseKeys(string value)
    {
        // Format: "<ckey> <ekey>" or just "<ckey>".
        // Some configs include multiple pairs; for our use-case we only take first pair.
        var parts = value.Split(' ', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
        if (parts.Length < 1)
            throw new InvalidDataException("Expected at least one hash key");

        var ckey = CascKey.ParseHex(parts[0]);
        if (parts.Length >= 2)
        {
            var ekey = CascKey.ParseHex(parts[1]);
            return (ckey, ekey);
        }

        return (ckey, null);
    }
}
