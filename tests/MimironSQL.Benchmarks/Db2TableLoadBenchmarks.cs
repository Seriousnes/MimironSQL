using Microsoft.EntityFrameworkCore;

using BenchmarkDotNet.Attributes;
using MimironSQL.Providers;
using MimironSQL.EntityFrameworkCore;

namespace MimironSQL.Benchmarks;

[MemoryDiagnoser]
public class Db2TableLoadBenchmarks
{
    private BenchmarkDb2Context _context = null!;

    [GlobalSetup]
    public void GlobalSetup()
    {
        var testDataDir = BenchmarkTestDataPaths.GetTestDataDirectory();

        var optionsBuilder = new DbContextOptionsBuilder<BenchmarkDb2Context>();
        optionsBuilder.UseMimironDb2(o => o.UseFileSystem(
            db2DirectoryPath: testDataDir,
            dbdDefinitionsDirectory: testDataDir));

        _context = new BenchmarkDb2Context(optionsBuilder.Options);
    }

    public static IEnumerable<Db2BenchmarkCase> Cases =>
    [
        new(
            "Map: Include MapChallengeModes, ToList",
            static context => context.Map
                .Include(m => m.MapChallengeModes)
                .ToList()
                .Count),

        new(
            "MapChallengeMode: Include Map, ToList",
            static context => context.MapChallengeMode
                .Include(m => m.Map)
                .ToList()
                .Count),

        new(
            "Spell: Projection (no Include), ToList",
            static context => context.Spell
                .Select(s => new SpellProjection(s.Id, s.NameSubtext_lang))
                .ToList()
                .Count),

        new(
            "Spell: Include SpellName + Projection, ToList",
            static context => context.Spell
                .Include(s => s.SpellName)
                .Select(s => new SpellProjection(s.Id, s.SpellName == null ? null : s.SpellName.Name_lang))
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
