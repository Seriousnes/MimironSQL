namespace MimironSQL.Providers;

public sealed record CascBuildInfoRecord(
    string Product,
    string? Branch,
    string BuildConfig,
    string? CdnConfig,
    string? Version);

public static class CascBuildInfo
{
    public static IReadOnlyList<CascBuildInfoRecord> Read(string buildInfoPath)
    {
        ArgumentNullException.ThrowIfNull(buildInfoPath);
        if (!File.Exists(buildInfoPath))
            throw new FileNotFoundException(".build.info not found", buildInfoPath);

        // .build.info can be tab-separated (older) or pipe-separated (modern). First non-empty line is header.
        // Columns vary slightly by era; we only need Product + BuildConfig.
        var lines = File.ReadAllLines(buildInfoPath);
        int headerLineIndex = -1;
        for (int i = 0; i < lines.Length; i++)
        {
            var line = lines[i].Trim();
            if (line.Length == 0)
                continue;

            headerLineIndex = i;
            break;
        }

        if (headerLineIndex < 0)
            return [];

        var headerLine = lines[headerLineIndex];
        char delimiter = DetectDelimiter(headerLine);
        var header = Split(headerLine, delimiter);
        var headerMap = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
        for (int i = 0; i < header.Length; i++)
        {
            var name = NormalizeHeaderName(header[i]);
            if (name.Length == 0)
                continue;
            headerMap[name] = i;
        }

        int productCol = FindColumn(headerMap, "Product", "product");
        int buildConfigCol = FindColumn(headerMap, "BuildConfig", "Build Key", "BuildKey", "Build Config", "buildconfig", "build_key");
        int branchCol = FindOptionalColumn(headerMap, "Branch", "branch");
        int cdnConfigCol = FindOptionalColumn(headerMap, "CDNConfig", "CDN Key", "CdnConfig", "cdnconfig", "cdn_key");
        int versionCol = FindOptionalColumn(headerMap, "Version", "version");

        var records = new List<CascBuildInfoRecord>();
        for (int i = headerLineIndex + 1; i < lines.Length; i++)
        {
            var line = lines[i].TrimEnd();
            if (line.Length == 0)
                continue;

            var cols = Split(line, delimiter);
            if (productCol >= cols.Length || buildConfigCol >= cols.Length)
                continue;

            var product = cols[productCol].Trim();
            var buildConfig = cols[buildConfigCol].Trim();
            if (product.Length == 0 || buildConfig.Length == 0)
                continue;

            // Normalize build config to hex (32 chars) if it contains a separator like '|'
            buildConfig = ExtractFirstHex(buildConfig) ?? buildConfig;

            string? branch = branchCol >= 0 && branchCol < cols.Length ? cols[branchCol].Trim() : null;
            string? cdnConfig = cdnConfigCol >= 0 && cdnConfigCol < cols.Length ? cols[cdnConfigCol].Trim() : null;
            cdnConfig = cdnConfig is { Length: > 0 } ? (ExtractFirstHex(cdnConfig) ?? cdnConfig) : null;

            string? version = versionCol >= 0 && versionCol < cols.Length ? cols[versionCol].Trim() : null;

            records.Add(new CascBuildInfoRecord(product, branch, buildConfig, cdnConfig, version));
        }

        return records;
    }

    public static CascBuildInfoRecord SelectForProduct(
        IReadOnlyList<CascBuildInfoRecord> records,
        string product,
        IEnumerable<string>? productFallbacks = null)
    {
        ArgumentNullException.ThrowIfNull(records);
        ArgumentNullException.ThrowIfNull(product);

        var wanted = new[] { product }.Concat(productFallbacks ?? []).ToArray();
        foreach (var p in wanted)
        {
            var record = records.FirstOrDefault(r => string.Equals(r.Product, p, StringComparison.OrdinalIgnoreCase));
            if (record is not null)
                return record;
        }

        // Some .build.info "Product" fields can include suffixes; allow prefix match.
        foreach (var p in wanted)
        {
            var record = records.FirstOrDefault(r => r.Product.StartsWith(p, StringComparison.OrdinalIgnoreCase));
            if (record is not null)
                return record;
        }

        throw new InvalidOperationException($"No .build.info record found for product '{product}'.");
    }

    private static int FindColumn(Dictionary<string, int> map, params string[] names)
    {
        foreach (var name in names)
            if (map.TryGetValue(name, out int idx))
                return idx;
        throw new InvalidDataException("Required column not found in .build.info.");
    }

    private static int FindOptionalColumn(Dictionary<string, int> map, params string[] names)
    {
        foreach (var name in names)
            if (map.TryGetValue(name, out int idx))
                return idx;
        return -1;
    }

    private static char DetectDelimiter(string headerLine)
    {
        if (headerLine.Contains('\t'))
            return '\t';
        if (headerLine.Contains('|'))
            return '|';
        return '\t';
    }

    private static string[] Split(string line, char delimiter) => line.Split(delimiter);

    private static string NormalizeHeaderName(string raw)
    {
        var trimmed = raw.Trim();

        // Modern .build.info encodes type info like "Product!STRING:0".
        int bang = trimmed.IndexOf('!');
        if (bang >= 0)
            trimmed = trimmed[..bang].Trim();

        return trimmed;
    }

    private static string? ExtractFirstHex(string input)
    {
        // Common patterns:
        // - just hex
        // - "ckey ekey" pairs, or "key|size" etc.
        // We only care about 32-hex substrings.
        for (int i = 0; i + 32 <= input.Length; i++)
        {
            var span = input.AsSpan(i, 32);
            if (IsHex32(span))
                return span.ToString().ToLowerInvariant();
        }
        return null;
    }

    private static bool IsHex32(ReadOnlySpan<char> s)
    {
        if (s.Length != 32) return false;
        for (int i = 0; i < s.Length; i++)
        {
            char c = s[i];
            bool isHex = (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F');
            if (!isHex) return false;
        }
        return true;
    }
}
