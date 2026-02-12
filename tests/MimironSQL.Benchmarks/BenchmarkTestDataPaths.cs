namespace MimironSQL.Benchmarks;

internal static class BenchmarkTestDataPaths
{
    public static bool TryGetTestDataDirectory(out string path)
    {
        var baseDir = new DirectoryInfo(AppContext.BaseDirectory);

        for (var current = baseDir; current is not null; current = current.Parent)
        {
            // Test data lives in: tests/TestData
            var candidate = Path.Combine(current.FullName, "tests", "TestData");
            if (Directory.Exists(candidate))
            {
                path = candidate;
                return true;
            }
        }

        path = string.Empty;
        return false;
    }

    public static string GetTestDataDirectory()
    {
        if (TryGetTestDataDirectory(out var path))
            return path;

        var baseDir = new DirectoryInfo(AppContext.BaseDirectory);
        throw new DirectoryNotFoundException($"Could not locate tests/TestData starting from '{baseDir.FullName}'.");
    }
}
