using BenchmarkDotNet.Attributes;
using MimironSQL.Providers;

namespace MimironSQL.Benchmarks;

[MemoryDiagnoser]
public class BlteDecodeBenchmarks
{
    private byte[] _blte = null!;

    [GlobalSetup]
    public void GlobalSetup()
    {
        if (!TryGetWowInstallRoot(out var wowInstallRoot) || !Directory.Exists(wowInstallRoot))
            throw new InvalidOperationException("WOW install root not found. Create .env.local with WOW_INSTALL_ROOT=... (same as integration tests). ");

        // Use a real CASC BLTE payload from the current WoW install.
        // The benchmark itself measures only `BlteDecoder.Decode` (no CASC IO), since we load bytes once.
        _blte = ReadEncodingBlteBytes(wowInstallRoot);
    }

    [Benchmark]
    public byte[] Decode()
        => BlteDecoder.Decode(_blte);

    private static byte[] ReadEncodingBlteBytes(string wowInstallRoot)
    {
        var buildInfo = CascBuildInfo.Open(wowInstallRoot);
        var flavor = buildInfo.GetFlavor("wow");

        var buildConfigKey = CascKey.ParseHex(flavor.BuildInfo.BuildConfig);
        var buildConfigBytes = CascConfigStore.ReadConfigBytes(flavor.DataConfigDirectory, buildConfigKey);
        var buildConfig = CascBuildConfigParser.Read(buildConfigBytes);

        var archiveReader = new CascLocalArchiveReader(flavor.DataDataDirectory);
        return archiveReader.ReadBlteBytesAsync(buildConfig.EncodingEKey ?? throw new InvalidOperationException("Build config did not include an ENCODING EKey."), CancellationToken.None)
            .ConfigureAwait(false)
            .GetAwaiter()
            .GetResult();
    }

    private static bool TryGetWowInstallRoot(out string wowInstallRoot)
    {
        var fromEnvironment = Environment.GetEnvironmentVariable("WOW_INSTALL_ROOT");
        if (!string.IsNullOrWhiteSpace(fromEnvironment))
        {
            wowInstallRoot = TrimOptionalQuotes(fromEnvironment.Trim());
            return wowInstallRoot.Length > 0;
        }

        if (!TryFindEnvLocalPath(out var path))
        {
            wowInstallRoot = string.Empty;
            return false;
        }

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

        wowInstallRoot = string.Empty;
        return false;
    }

    private static bool TryFindEnvLocalPath(out string path)
    {
        // BenchmarkDotNet executes in a generated artifacts folder, so base-directory-relative lookups
        // won't find the repo's `.env.local`. Prefer locating the repo root and checking known project paths.
        if (TryFindRepoRoot(out var repoRoot))
        {
            var candidates = new[]
            {
                Path.Combine(repoRoot, "tests", "MimironSQL.Benchmarks", ".env.local"),
                Path.Combine(repoRoot, "tests", "MimironSQL.Profiling", ".env.local"),
                Path.Combine(repoRoot, "tests", "MimironSQL.IntegrationTests", ".env.local"),
                Path.Combine(repoRoot, ".env.local"),
            };

            foreach (var candidate in candidates)
            {
                if (File.Exists(candidate))
                {
                    path = candidate;
                    return true;
                }
            }
        }

        // Fallback: walk up from the current base directory.
        var baseDir = new DirectoryInfo(AppContext.BaseDirectory);
        for (var current = baseDir; current is not null; current = current.Parent)
        {
            var candidate = Path.Combine(current.FullName, ".env.local");
            if (File.Exists(candidate))
            {
                path = candidate;
                return true;
            }
        }

        path = string.Empty;
        return false;
    }

    private static bool TryFindRepoRoot(out string repoRoot)
    {
        var baseDir = new DirectoryInfo(AppContext.BaseDirectory);
        for (var current = baseDir; current is not null; current = current.Parent)
        {
            if (File.Exists(Path.Combine(current.FullName, "MimironSQL.slnx")))
            {
                repoRoot = current.FullName;
                return true;
            }
        }

        repoRoot = string.Empty;
        return false;
    }

    private static string TrimOptionalQuotes(string value)
        => value is ['\"', .., '\"'] or ['\'', .., '\''] ? value[1..^1].Trim() : value;
}
