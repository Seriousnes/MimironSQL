namespace MimironSQL.Providers;

/// <summary>
/// Describes relevant CASC paths within a World of Warcraft installation.
/// </summary>
/// <param name="InstallRoot">Root directory of the installation.</param>
/// <param name="FlavorDirectory">The detected flavor directory (for example <c>_retail_</c>).</param>
/// <param name="DataDirectory">The <c>Data</c> directory.</param>
/// <param name="DataDataDirectory">The <c>Data\data</c> directory.</param>
/// <param name="DataConfigDirectory">The <c>Data\config</c> directory.</param>
/// <param name="Product">The CASC product token (for example <c>wow</c>).</param>
/// <param name="BuildInfoPath">Path to the <c>.build.info</c> file.</param>
/// <param name="FlavorInfoPath">Path to the <c>.flavor.info</c> file (when present).</param>
internal sealed record CascInstallLayout(
    string InstallRoot,
    string FlavorDirectory,
    string DataDirectory,
    string DataDataDirectory,
    string DataConfigDirectory,
    string Product,
    string BuildInfoPath,
    string FlavorInfoPath);

/// <summary>
/// Detects CASC directory layout for a given World of Warcraft install root.
/// </summary>
internal static class CascInstallLayoutDetector
{
    /// <summary>
    /// Detects the CASC install layout under the provided install root.
    /// </summary>
    /// <param name="installRoot">Root directory of the World of Warcraft installation.</param>
    /// <returns>The detected install layout.</returns>
    public static CascInstallLayout Detect(string installRoot)
    {
        ArgumentNullException.ThrowIfNull(installRoot);
        if (!Directory.Exists(installRoot))
            throw new DirectoryNotFoundException(installRoot);

        // .build.info may live in shared-storage root (installRoot) or inside flavor dir.
        var buildInfoCandidates = new[]
        {
            Path.Combine(installRoot, ".build.info"),
        };

        string? buildInfoPath = buildInfoCandidates.FirstOrDefault(File.Exists) ?? throw new FileNotFoundException(".build.info not found in install root", Path.Combine(installRoot, ".build.info"));

        // Shared-storage layout: <root>\_retail_ (or similar)\Data\...
        // Since Flavor is obsolete, auto-detect a suitable flavor directory.
        foreach (var candidateDir in Directory.EnumerateDirectories(installRoot))
        {
            var dirName = Path.GetFileName(candidateDir);
            if (string.IsNullOrWhiteSpace(dirName) || !dirName.StartsWith('_'))
                continue;

            var dataDir = Path.Combine(candidateDir, "Data");
            var dataDataDir = Path.Combine(dataDir, "data");
            var dataConfigDir = Path.Combine(dataDir, "config");
            if (!Directory.Exists(dataDataDir) || !Directory.Exists(dataConfigDir))
                continue;

            var flavorInfo = Path.Combine(candidateDir, ".flavor.info");
            var product = File.Exists(flavorInfo) ? CascFlavorInfo.ReadProduct(flavorInfo) : "wow";

            return new CascInstallLayout(installRoot, candidateDir, dataDir, dataDataDir, dataConfigDir, product, buildInfoPath, flavorInfo);
        }

        // Non-shared layout: Data is directly under installRoot.
        {
            var dataDir = Path.Combine(installRoot, "Data");
            var dataDataDir = Path.Combine(dataDir, "data");
            var dataConfigDir = Path.Combine(dataDir, "config");

            if (Directory.Exists(dataDataDir) && Directory.Exists(dataConfigDir))
            {
                // If caller passed the flavor dir itself, flavor info might be here.
                var flavorInfo = Path.Combine(installRoot, ".flavor.info");
                var product = File.Exists(flavorInfo) ? CascFlavorInfo.ReadProduct(flavorInfo) : "wow";

                return new CascInstallLayout(installRoot, installRoot, dataDir, dataDataDir, dataConfigDir, product, buildInfoPath, flavorInfo);
            }
        }

        throw new InvalidOperationException("Unable to locate WoW Data folders (Data\\data + Data\\config) under the provided install root.");
    }
}
