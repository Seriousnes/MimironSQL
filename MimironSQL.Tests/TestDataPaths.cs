using System;
using System.IO;

namespace MimironSQL.Tests;

internal static class TestDataPaths
{
    public static FileStream OpenMapDb2() => File.OpenRead(GetMapDb2Path());
    public static FileStream OpenSpellDb2() => File.OpenRead(GetSpellDb2Path());
    public static FileStream OpenCollectableSourceQuestSparseDb2() => File.OpenRead(GetCollectableSourceQuestSparseDb2Path());

    private static string GetMapDb2Path()
    {
        var baseDir = AppContext.BaseDirectory;
        var path = Path.GetFullPath(Path.Combine(baseDir, "..", "..", "..", "TestData", "map.db2"));
        EnsureExists(path);
        return path;
    }

    private static string GetSpellDb2Path()
    {
        var baseDir = AppContext.BaseDirectory;
        var path = Path.GetFullPath(Path.Combine(baseDir, "..", "..", "..", "TestData", "spell.db2"));
        EnsureExists(path);
        return path;
    }

    private static string GetCollectableSourceQuestSparseDb2Path()
    {
        var baseDir = AppContext.BaseDirectory;
        var path = Path.GetFullPath(Path.Combine(baseDir, "..", "..", "..", "TestData", "collectablesourcequestsparse.db2"));
        EnsureExists(path);
        return path;
    }

    private static void EnsureExists(string path)
    {
        if (!File.Exists(path))
            throw new FileNotFoundException($"Missing test data file: {path}");
    }
}
