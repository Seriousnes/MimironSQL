namespace MimironSQL.Providers;

public sealed record CascInstallLayout(
    string InstallRoot,
    string FlavorDirectory,
    string DataDirectory,
    string DataDataDirectory,
    string DataConfigDirectory,
    string Product,
    string BuildInfoPath,
    string FlavorInfoPath);

public static class CascInstallLayoutDetector
{
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
