using BenchmarkDotNet.Attributes;
using MimironSQL.Providers;

namespace MimironSQL.Benchmarks;

[MemoryDiagnoser]
public class Db2TableLoadBenchmarks
{
    private BenchmarkDb2Context _context = null!;

    [GlobalSetup]
    public void GlobalSetup()
    {
        var testDataDir = BenchmarkTestDataPaths.GetTestDataDirectory();

        var db2Provider = new FileSystemDb2StreamProvider(new(testDataDir));
        var dbdProvider = new FileSystemDbdProvider(new(testDataDir));

        _context = new BenchmarkDb2Context(dbdProvider, db2Provider);
    }

    public static IEnumerable<Db2BenchmarkCase> Cases =>
    [
        new(
            "Map: Include MapChallengeModes, ToList",
            static context => context.Map
                .ToList()
                .Count),

        new(
            "MapChallengeMode: Include FirstRewardQuest, ToList",
            static context => context.MapChallengeMode
                .ToList()
                .Count),

        new(
            "Spell: Projection (no Include), ToList",
            static context => context.Spell
                .Select(s => new SpellProjection(s.Id, s.SpellName.Name_lang))
                .ToList()
                .Count),

        new(
            "Spell: Include SpellName + Projection, ToList",
            static context => context.Spell
                .Select(s => new SpellProjection(s.Id, s.SpellName.Name_lang))
                .ToList()
                .Count),
    ];

    [Benchmark]
    [ArgumentsSource(nameof(Cases))]
    public int Run(Db2BenchmarkCase benchmark)
        => benchmark.Run(_context);

    public sealed record Db2BenchmarkCase(string Name, Func<BenchmarkDb2Context, int> Run)
    {
        public override string ToString() => Name;
    }

    public readonly record struct SpellProjection(int Id, string? Name);
}
