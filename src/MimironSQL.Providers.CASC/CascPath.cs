namespace MimironSQL.Providers;

/// <summary>
/// Helpers for normalizing logical CASC paths.
/// </summary>
internal static class CascPath
{
    /// <summary>
    /// Normalizes an input path to a canonical CASC logical path.
    /// </summary>
    /// <param name="path">The input path.</param>
    /// <returns>A normalized CASC path using backslashes and no leading separator.</returns>
    public static string NormalizeCascPath(string path)
    {
        ArgumentNullException.ThrowIfNull(path);

        // CASC logical paths are case-insensitive; allow either separator.
        var normalized = path.Replace('/', '\\').TrimStart('\\');
        return normalized;
    }

    /// <summary>
    /// Normalizes an input path to a canonical DB2 path under <c>DBFilesClient\</c>.
    /// </summary>
    /// <param name="path">The input DB2 path.</param>
    /// <returns>A normalized DB2 path starting with <c>DBFilesClient\</c>.</returns>
    public static string NormalizeDb2Path(string path)
    {
        var normalized = NormalizeCascPath(path);

        // Canonicalize DBFilesClient prefix casing.
        const string prefix = "DBFilesClient\\";
        if (!normalized.StartsWith(prefix, StringComparison.OrdinalIgnoreCase))
            throw new ArgumentException("DB2 paths must start with 'DBFilesClient\\\\'.", nameof(path));

        return prefix + normalized[prefix.Length..];
    }
}
