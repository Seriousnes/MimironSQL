using Microsoft.EntityFrameworkCore;

using MimironSQL.EntityFrameworkCore;

namespace MimironSQL.IntegrationTests.Helpers;

internal static class TestHelpers
{
    private static readonly Lazy<string> WowVersionLazy = new(ResolveWowVersion);

    public static string WowVersion => WowVersionLazy.Value;

    public static DbContextOptionsBuilder<TContext> UseMimironDb2ForTests<TContext>(
        this DbContextOptionsBuilder<TContext> optionsBuilder,
        Action<IMimironDb2DbContextOptionsBuilder> configure)
        where TContext : DbContext
    {
        ArgumentNullException.ThrowIfNull(optionsBuilder);
        ArgumentNullException.ThrowIfNull(configure);        

        optionsBuilder.UseMimironDb2(o =>
        {
            o.WithWowVersion(WowVersion);
            configure(o);
        });

        return optionsBuilder;
    }

    public static string CreateCustomIndexCacheDirectory(string scope)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(scope);

        var path = Path.Combine(
            Path.GetTempPath(),
            "MimironSQL.IntegrationTests",
            "custom-indexes",
            scope,
            Guid.NewGuid().ToString("N"));

        Directory.CreateDirectory(path);
        return path;
    }

    public static void DeleteDirectoryIfExists(string path)
    {
        if (string.IsNullOrWhiteSpace(path) || !Directory.Exists(path))
        {
            return;
        }

        Directory.Delete(path, recursive: true);
    }

    private static string ResolveWowVersion()
    {
        var env = Environment.GetEnvironmentVariable("WOW_VERSION");
        if (!string.IsNullOrWhiteSpace(env))
        {
            return env.Trim();
        }

        var repoRoot = FindRepoRoot();
        var envPath = Path.Combine(repoRoot, "tests", "MimironSQL.IntegrationTests", ".env");
        if (!File.Exists(envPath))
        {
            throw new InvalidOperationException(
                $"WOW_VERSION is not set and no .env file was found at '{envPath}'. " +
                "Set WOW_VERSION or create tests/MimironSQL.IntegrationTests/.env with WOW_VERSION=<version>.");
        }

        foreach (var rawLine in File.ReadLines(envPath))
        {
            var line = rawLine.Trim();
            if (line.Length == 0 || line.StartsWith('#'))
            {
                continue;
            }

            var equals = line.IndexOf('=');
            if (equals <= 0)
            {
                continue;
            }

            var key = line[..equals].Trim();
            if (!string.Equals(key, "WOW_VERSION", StringComparison.OrdinalIgnoreCase))
            {
                continue;
            }

            var value = line[(equals + 1)..].Trim();
            if (value.Length >= 2 && value[0] == '"' && value[^1] == '"')
            {
                value = value[1..^1];
            }

            if (string.IsNullOrWhiteSpace(value))
            {
                break;
            }

            return value;
        }

        throw new InvalidOperationException(
            $"No WOW_VERSION entry was found in '{envPath}'. " +
            "Add WOW_VERSION=<version> or set the WOW_VERSION environment variable.");
    }

    private static string FindRepoRoot()
    {
        var dir = new DirectoryInfo(AppContext.BaseDirectory);
        for (var i = 0; i < 20 && dir is not null; i++)
        {
            if (File.Exists(Path.Combine(dir.FullName, "MimironSQL.slnx")))
            {
                return dir.FullName;
            }

            dir = dir.Parent;
        }

        throw new InvalidOperationException(
            $"Failed to locate repository root from '{AppContext.BaseDirectory}'. " +
            "Expected to find 'MimironSQL.slnx' in a parent directory.");
    }
}
