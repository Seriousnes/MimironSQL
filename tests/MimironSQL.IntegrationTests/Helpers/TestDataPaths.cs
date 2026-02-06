namespace MimironSQL.IntegrationTests.Helpers;

internal static class TestDataPaths
{
    public static string GetTestDataDirectory()
    {
        var baseDir = AppContext.BaseDirectory;
        return Path.GetFullPath(Path.Combine(baseDir, "..", "..", "..", "TestData"));
    }
}
