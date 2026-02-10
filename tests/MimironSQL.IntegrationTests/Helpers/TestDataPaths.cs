namespace MimironSQL.IntegrationTests.Helpers;

internal static class TestDataPaths
{
    public static string GetTestDataDirectory()
    {
        var baseDir = AppContext.BaseDirectory;
        return System.IO.Path.GetFullPath(System.IO.Path.Combine(baseDir, "..", "..", "..", "TestData"));
    }
}
