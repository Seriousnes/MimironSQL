using System;
using System.IO;

namespace MimironSQL.Tests;

internal static class TestDataPaths
{
    public static string MapDb2 => GetTestDataPath("map.db2");
    public static string SpellDb2 => GetTestDataPath("spell.db2");

    private static string GetTestDataPath(string fileName)
    {
        var baseDir = AppContext.BaseDirectory;
        var path = Path.GetFullPath(Path.Combine(baseDir, "..", "..", "..", "TestData", fileName));
        if (!File.Exists(path))
            throw new FileNotFoundException($"Missing test data file: {path}");
        return path;
    }
}
