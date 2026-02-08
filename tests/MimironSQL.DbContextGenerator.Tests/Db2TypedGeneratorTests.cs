using System.Collections.Immutable;
using System.Text;

using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.Text;

using MimironSQL.DbContextGenerator.Tests.Helpers;

using Shouldly;

namespace MimironSQL.DbContextGenerator.Tests;

public sealed class DbContextGeneratorSmokeTests
{
    [Fact]
    public void Generator_can_be_constructed()
    {
        var generator = new MimironSQL.DbContextGenerator.DbContextGenerator();
        generator.ShouldNotBeNull();
    }

    [Fact]
    public void Generator_emits_reference_navigations_and_skips_fk_arrays()
    {
        var env = "WOW_VERSION=1.0.0.1\n";

        var mapDbd = """
COLUMNS
int ID

BUILD 1.0.0.1
$id$ID<32>
""";

        var questDbd = """
COLUMNS
int ID

BUILD 1.0.0.1
$id$ID<32>
""";

        // Note: MapID is a reference but not marked $relation$ in the build.
        var sourceDbd = """
COLUMNS
int ID
int<Map::ID> MapID
int<QuestV2::ID> FirstRewardQuestID

BUILD 1.0.0.1
$id$ID<32>
MapID<u16>
FirstRewardQuestID<32>[6]
""";

        var results = RunGenerator(
            additionalFiles:
            [
                (".env", env),
                ("Map.dbd", mapDbd),
                ("QuestV2.dbd", questDbd),
                ("MapChallengeMode.dbd", sourceDbd),
            ]);

        var mapChallengeMode = results.Single(s => s.HintName.EndsWith("MapChallengeMode.g.cs", StringComparison.Ordinal));
        mapChallengeMode.SourceText.ShouldContain("public int MapID { get; set; }");
        mapChallengeMode.SourceText.ShouldContain("public virtual Map? Map { get; set; }");
        mapChallengeMode.SourceText.ShouldNotContain("MapIDKey");

        mapChallengeMode.SourceText.ShouldNotContain("[ForeignKey(");
        mapChallengeMode.SourceText.ShouldNotContain("FirstRewardQuest");

        var dbContext = results.Single(s => s.HintName.EndsWith("WoWDb2Context.g.cs", StringComparison.Ordinal));
        dbContext.SourceText.ShouldContain("modelBuilder.ApplyConfigurationsFromAssembly(typeof(WoWDb2Context).Assembly);");
        dbContext.SourceText.ShouldContain("OnModelCreatingPartial(modelBuilder);");

        var mapChallengeModeConfig = results.Single(s => s.HintName.EndsWith("MapChallengeModeConfiguration.g.cs", StringComparison.Ordinal));
        mapChallengeModeConfig.SourceText.ShouldContain("builder.HasOne(x => x.Map).WithMany().HasForeignKey(x => x.MapID);");
    }

    [Fact]
    public void Generator_appends_Entity_suffix_on_reference_collision()
    {
        var env = "WOW_VERSION=1.0.0.1\n";

        var mapDbd = """
COLUMNS
int ID

BUILD 1.0.0.1
$id$ID<32>
""";

        // Column name "Map" (no ID suffix) should produce Map + MapEntity.
        var sourceDbd = """
COLUMNS
int ID
int<Map::ID> Map

BUILD 1.0.0.1
$id$ID<32>
Map<32>
""";

        var results = RunGenerator(
            additionalFiles:
            [
                (".env", env),
                ("Map.dbd", mapDbd),
                ("Foo.dbd", sourceDbd),
            ]);

        var foo = results.Single(s => s.HintName.EndsWith("Foo.g.cs", StringComparison.Ordinal));
        foo.SourceText.ShouldContain("public int Map { get; set; }");
        foo.SourceText.ShouldContain("public virtual Map? MapEntity { get; set; }");
    }

    private static ImmutableArray<(string HintName, string SourceText)> RunGenerator((string Path, string Content)[] additionalFiles)
    {
        var parseOptions = new CSharpParseOptions(LanguageVersion.Preview);

        var compilation = CSharpCompilation.Create(
            assemblyName: "GeneratorTests",
            syntaxTrees: [CSharpSyntaxTree.ParseText("namespace MimironSQL; public sealed class Dummy {}", parseOptions)],
            references: GetReferences(),
            options: new CSharpCompilationOptions(OutputKind.DynamicallyLinkedLibrary));

        ImmutableArray<AdditionalText> additionalTexts =
        [
            .. additionalFiles.Select(f => (AdditionalText)new InMemoryAdditionalText(
                path: f.Path,
                text: SourceText.From(f.Content, Encoding.UTF8)))
        ];

        var generator = new MimironSQL.DbContextGenerator.DbContextGenerator();
        GeneratorDriver driver = CSharpGeneratorDriver.Create(
            generators: [generator.AsSourceGenerator()],
            additionalTexts: additionalTexts,
            parseOptions: parseOptions,
            optionsProvider: new TestAnalyzerConfigOptionsProvider(ImmutableDictionary<string, string>.Empty));

        driver = driver.RunGenerators(compilation);

        var runResult = driver.GetRunResult();
        runResult.Diagnostics.Where(d => d.Severity == DiagnosticSeverity.Error).ShouldBeEmpty();

        var sources = runResult.Results
            .Single()
            .GeneratedSources
            .Select(s => (s.HintName, s.SourceText.ToString()))
            .ToImmutableArray();

        sources.Length.ShouldBeGreaterThan(0);
        return sources;
    }

    private static ImmutableArray<MetadataReference> GetReferences()
    {
        var references = new List<MetadataReference>
        {
            MetadataReference.CreateFromFile(typeof(object).Assembly.Location),
            MetadataReference.CreateFromFile(typeof(Enumerable).Assembly.Location),
            MetadataReference.CreateFromFile(typeof(System.ComponentModel.DataAnnotations.Schema.ForeignKeyAttribute).Assembly.Location),
        };

        return [.. references];
    }
}
