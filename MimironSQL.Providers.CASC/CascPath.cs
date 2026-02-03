namespace MimironSQL.Providers;

public static class CascPath
{
    public static string NormalizeCascPath(string path)
    {
        ArgumentNullException.ThrowIfNull(path);

        // CASC logical paths are case-insensitive; allow either separator.
        var normalized = path.Replace('/', '\\').TrimStart('\\');
        return normalized;
    }

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
