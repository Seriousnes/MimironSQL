namespace MimironSQL.Benchmarks;

internal static class BenchmarkTestDataPaths
{
    public static string GetTestDataDirectory()
    {
        var baseDir = new DirectoryInfo(AppContext.BaseDirectory);

        for (var current = baseDir; current is not null; current = current.Parent)
        {
            var candidate = Path.Combine(current.FullName, "MimironSQL.Tests", "TestData");
            if (Directory.Exists(candidate))
                return candidate;
        }

        throw new DirectoryNotFoundException($"Could not locate MimironSQL.Tests/TestData starting from '{baseDir.FullName}'.");
    }
}
