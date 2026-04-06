namespace MimironSQL.Providers;

/// <summary>
/// Represents a single row from the <c>.build.info</c> file.
/// </summary>
/// <param name="Product">The product identifier.</param>
/// <param name="Branch">The branch identifier, when present.</param>
/// <param name="BuildConfig">The build config key.</param>
/// <param name="CdnConfig">The CDN config key, when present.</param>
/// <param name="Version">The version string, when present.</param>
internal sealed record CascBuildInfoRecord(
    string Product,
    string? Branch,
    string BuildConfig,
    string? CdnConfig,
    string? Version);

/// <summary>
/// Describes a detected WoW flavor (e.g. <c>_retail_</c>) paired with its <c>.build.info</c> record.
/// </summary>
/// <param name="Product">The CASC product token (e.g. <c>wow</c>).</param>
/// <param name="FlavorDirectory">The flavor root directory (e.g. <c>_retail_</c>).</param>
/// <param name="DataDirectory">The <c>Data</c> directory under the flavor.</param>
/// <param name="DataDataDirectory">The <c>Data\data</c> directory.</param>
/// <param name="DataConfigDirectory">The <c>Data\config</c> directory.</param>
/// <param name="BuildInfo">The matched <c>.build.info</c> record for this flavor's product.</param>
internal sealed record CascFlavor(
    string Product,
    string FlavorDirectory,
    string DataDirectory,
    string DataDataDirectory,
    string DataConfigDirectory,
    CascBuildInfoRecord BuildInfo);

/// <summary>
/// Reads and correlates <c>.build.info</c> and per-flavor <c>.flavor.info</c> from a WoW installation.
/// </summary>
internal sealed class CascBuildInfo
{
    private CascBuildInfo(string installRoot, IReadOnlyList<CascBuildInfoRecord> records, IReadOnlyList<CascFlavor> flavors)
    {
        InstallRoot = installRoot;
        Records = records;
        Flavors = flavors;
    }

    /// <summary>
    /// Root directory of the World of Warcraft installation.
    /// </summary>
    public string InstallRoot { get; }

    /// <summary>
    /// All parsed rows from the <c>.build.info</c> file.
    /// </summary>
    public IReadOnlyList<CascBuildInfoRecord> Records { get; }

    /// <summary>
    /// Detected flavors, each paired with its matched <c>.build.info</c> record.
    /// </summary>
    public IReadOnlyList<CascFlavor> Flavors { get; }

    /// <summary>
    /// Gets the flavor matching the specified product token.
    /// </summary>
    /// <param name="product">The CASC product token (e.g. <c>wow</c>, <c>wowt</c>, <c>wow_classic</c>).</param>
    /// <returns>The matching <see cref="CascFlavor"/>.</returns>
    /// <exception cref="InvalidOperationException">No flavor matches the specified product.</exception>
    public CascFlavor GetFlavor(string product)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(product);

        foreach (var flavor in Flavors)
        {
            if (string.Equals(flavor.Product, product, StringComparison.OrdinalIgnoreCase))
            {
                return flavor;
            }
        }

        var available = string.Join(", ", Flavors.Select(f => f.Product));
        throw new InvalidOperationException($"No flavor found for product '{product}'. Available: {available}");
    }

    /// <summary>
    /// Opens and parses the <c>.build.info</c> from a WoW installation root, detecting all flavors.
    /// </summary>
    /// <param name="installRoot">Root directory of the World of Warcraft installation.</param>
    /// <returns>A <see cref="CascBuildInfo"/> with parsed records and detected flavors.</returns>
    public static CascBuildInfo Open(string installRoot)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(installRoot);

        var buildInfoPath = Directory.GetFiles(installRoot, ".build.info", SearchOption.TopDirectoryOnly)
            is [var path]
                ? path
                : throw new FileNotFoundException(".build.info not found in install root", Path.Combine(installRoot, ".build.info"));

        var records = ReadBuildInfo(buildInfoPath);

        // Detect flavor directories by searching for .flavor.info files in immediate subdirectories.
        var flavorInfoPaths = Directory.GetFiles(installRoot, ".flavor.info", SearchOption.AllDirectories);
        var flavors = new List<CascFlavor>();

        foreach (var flavorInfoPath in flavorInfoPaths)
        {
            var flavorDir = Path.GetDirectoryName(flavorInfoPath)!;
            var dataDir = Path.Combine(flavorDir, "Data");
            var dataDataDir = Path.Combine(dataDir, "data");
            var dataConfigDir = Path.Combine(dataDir, "config");

            if (!Directory.Exists(dataDataDir) || !Directory.Exists(dataConfigDir))
            {
                continue;
            }

            var product = ReadFlavorProduct(flavorInfoPath);
            var record = SelectForProduct(records, product);
            flavors.Add(new CascFlavor(product, flavorDir, dataDir, dataDataDir, dataConfigDir, record));
        }

        // Fallback: Data directory directly under installRoot (non-shared layout).
        if (flavors.Count == 0)
        {
            var dataDir = Path.Combine(installRoot, "Data");
            var dataDataDir = Path.Combine(dataDir, "data");
            var dataConfigDir = Path.Combine(dataDir, "config");

            if (Directory.Exists(dataDataDir) && Directory.Exists(dataConfigDir))
            {
                var product = "wow";
                var flavorInfoPath = Path.Combine(installRoot, ".flavor.info");
                if (File.Exists(flavorInfoPath))
                {
                    product = ReadFlavorProduct(flavorInfoPath);
                }

                var record = SelectForProduct(records, product);
                flavors.Add(new CascFlavor(product, installRoot, dataDir, dataDataDir, dataConfigDir, record));
            }
        }

        if (flavors.Count == 0)
        {
            throw new InvalidOperationException("Unable to locate WoW Data folders (Data\\data + Data\\config) under the provided install root.");
        }

        return new CascBuildInfo(installRoot, records, flavors);
    }

    /// <summary>
    /// Selects the first record matching the specified product, optionally falling back to additional products.
    /// </summary>
    /// <param name="records">The available records.</param>
    /// <param name="product">The desired product identifier.</param>
    /// <param name="productFallbacks">Optional additional products to try.</param>
    /// <returns>The selected build info record.</returns>
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
            {
                return record;
            }
        }

        // Some .build.info "Product" fields can include suffixes; allow prefix match.
        foreach (var p in wanted)
        {
            var record = records.FirstOrDefault(r => r.Product.StartsWith(p, StringComparison.OrdinalIgnoreCase));
            if (record is not null)
            {
                return record;
            }
        }

        throw new InvalidOperationException($"No .build.info record found for product '{product}'.");
    }

    private static string ReadFlavorProduct(string flavorInfoPath)
    {
        var text = File.ReadAllText(flavorInfoPath).Trim();
        return text.Length > 0 ? text : "wow";
    }

    private static IReadOnlyList<CascBuildInfoRecord> ReadBuildInfo(string buildInfoPath)
    {
        var lines = File.ReadAllLines(buildInfoPath);
        int headerLineIndex = -1;
        for (int i = 0; i < lines.Length; i++)
        {
            var line = lines[i].Trim();
            if (line.Length == 0)
            {
                continue;
            }

            headerLineIndex = i;
            break;
        }

        if (headerLineIndex < 0)
        {
            return [];
        }

        var headerLine = lines[headerLineIndex];
        char delimiter = DetectDelimiter(headerLine);
        var header = Split(headerLine, delimiter);
        var headerMap = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
        for (int i = 0; i < header.Length; i++)
        {
            var name = NormalizeHeaderName(header[i]);
            if (name.Length == 0)
            {
                continue;
            }

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
            {
                continue;
            }

            var cols = Split(line, delimiter);
            if (productCol >= cols.Length || buildConfigCol >= cols.Length)
            {
                continue;
            }

            var product = cols[productCol].Trim();
            var buildConfig = cols[buildConfigCol].Trim();
            if (product.Length == 0 || buildConfig.Length == 0)
            {
                continue;
            }

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

    private static int FindColumn(Dictionary<string, int> map, params string[] names)
    {
        foreach (var name in names)
        {
            if (map.TryGetValue(name, out int idx))
            {
                return idx;
            }
        }

        throw new InvalidDataException("Required column not found in .build.info.");
    }

    private static int FindOptionalColumn(Dictionary<string, int> map, params string[] names)
    {
        foreach (var name in names)
        {
            if (map.TryGetValue(name, out int idx))
            {
                return idx;
            }
        }

        return -1;
    }

    private static char DetectDelimiter(string headerLine)
    {
        if (headerLine.Contains('\t'))
        {
            return '\t';
        }

        if (headerLine.Contains('|'))
        {
            return '|';
        }

        return '\t';
    }

    private static string[] Split(string line, char delimiter) => line.Split(delimiter);

    private static string NormalizeHeaderName(string raw)
    {
        var trimmed = raw.Trim();

        // Modern .build.info encodes type info like "Product!STRING:0".
        int bang = trimmed.IndexOf('!');
        if (bang >= 0)
        {
            trimmed = trimmed[..bang].Trim();
        }

        return trimmed;
    }

    private static string? ExtractFirstHex(string input)
    {
        for (int i = 0; i + 32 <= input.Length; i++)
        {
            var span = input.AsSpan(i, 32);
            if (IsHex32(span))
            {
                return span.ToString().ToLowerInvariant();
            }
        }
        return null;
    }

    private static bool IsHex32(ReadOnlySpan<char> s)
    {
        if (s.Length != 32)
        {
            return false;
        }

        for (int i = 0; i < s.Length; i++)
        {
            char c = s[i];
            bool isHex = (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F');
            if (!isHex)
            {
                return false;
            }
        }
        return true;
    }
}
