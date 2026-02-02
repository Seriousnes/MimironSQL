namespace CASC.Net.Generators;

internal static class PathFlavorResolver
{
    public static string? TryGetFlavor(string filePath, string wowDbDefsRoot)
    {
        if (string.IsNullOrWhiteSpace(filePath))
            return null;

        if (string.IsNullOrWhiteSpace(wowDbDefsRoot))
            return null;

        var root = wowDbDefsRoot.TrimEnd('\\', '/');
        if (root.Length == 0)
            return null;

        // Require immediate child folder under root.
        var prefix = root + "\\";
        if (!filePath.StartsWith(prefix, StringComparison.OrdinalIgnoreCase))
        {
            prefix = root + "/";
            if (!filePath.StartsWith(prefix, StringComparison.OrdinalIgnoreCase))
                return null;
        }

        var remainder = filePath.Substring(prefix.Length);
        if (remainder.Length == 0)
            return null;

        var sepIndex = remainder.IndexOfAny(new[] { '\\', '/' });
        if (sepIndex <= 0)
            return null;

        var segment = remainder.Substring(0, sepIndex).Trim();
        return segment.Length == 0 ? null : segment;
    }
}
