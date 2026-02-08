using BenchmarkDotNet.Attributes;

using Microsoft.EntityFrameworkCore;

using MimironSQL.EntityFrameworkCore.Db2.Query;
using MimironSQL.EntityFrameworkCore;
using MimironSQL.Providers;

namespace MimironSQL.Benchmarks;

[MemoryDiagnoser]
public class Db2AllocationBenchmarks
{
    private const int ColdOperationsPerInvoke = 8;

    private string _testDataDir = null!;

    private BenchmarkDb2Context _warmContext = null!;

    [GlobalSetup]
    public void GlobalSetup()
    {
        _testDataDir = BenchmarkTestDataPaths.GetTestDataDirectory();

        _warmContext = CreateContext(_testDataDir);

        _ = _warmContext.SpellName
            .Where(x => x.Name_lang.Contains("an"))
            .Select(x => x.Id)
            .ToList();
    }

    [GlobalCleanup]
    public void GlobalCleanup()
    {
        _warmContext?.Dispose();
    }

    [Benchmark]
    public int Baseline_Spell_SelectId_ToListCount()
        => _warmContext.Spell
            .Select(x => x.Id)
            .ToList()
            .Count;

    [Benchmark]
    public int Projection_SpellName_SelectName_ToListCount()
        => _warmContext.SpellName
            .Select(x => x.Name_lang)
            .ToList()
            .Count;

    [Benchmark]
    public int Include_Map_WithChallengeModes_ToListCount()
        => _warmContext.Map
            .Include(x => x.MapChallengeModes)
            .ToList()
            .Count;

    [Benchmark]
    public int DenseString_WarmContains_SpellName_ToListCount()
        => _warmContext.SpellName
            .Where(x => x.Name_lang.Contains("an"))
            .Select(x => x.Id)
            .ToList()
            .Count;

    [Benchmark(OperationsPerInvoke = ColdOperationsPerInvoke)]
    public int DenseString_ColdContains_SpellName_ToListCount()
    {
        var total = 0;

        for (var i = 0; i < ColdOperationsPerInvoke; i++)
        {
            Db2DenseStringScanner.ClearCacheForTesting();

            total += _warmContext.SpellName
                .Where(x => x.Name_lang.Contains("an"))
                .Select(x => x.Id)
                .ToList()
                .Count;
        }

        return total;
    }

    private static BenchmarkDb2Context CreateContext(string testDataDir)
    {
        var db2Provider = new FileSystemDb2StreamProvider(new(testDataDir));
        var dbdProvider = new FileSystemDbdProvider(new(testDataDir));

        var optionsBuilder = new DbContextOptionsBuilder<BenchmarkDb2Context>();
        optionsBuilder.UseMimironDb2(db2Provider, dbdProvider, new NullTactKeyProvider());

        return new BenchmarkDb2Context(optionsBuilder.Options);
    }

    private sealed class NullTactKeyProvider : ITactKeyProvider
    {
        public bool TryGetKey(ulong keyName, out ReadOnlyMemory<byte> key)
        {
            key = default;
            return false;
        }
    }
}
