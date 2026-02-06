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
    public void Generator_emits_reference_and_fk_array_navigations_from_columns()
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
        mapChallengeMode.SourceText.ShouldContain("public ushort MapID { get; set; }");
        mapChallengeMode.SourceText.ShouldContain("[ForeignKey(nameof(MapID))]");
        mapChallengeMode.SourceText.ShouldContain("public Map? Map { get; set; }");

        mapChallengeMode.SourceText.ShouldContain("public ICollection<int> FirstRewardQuestID { get; set; } = [];");
        mapChallengeMode.SourceText.ShouldContain("[ForeignKey(nameof(FirstRewardQuestID))]");
        mapChallengeMode.SourceText.ShouldContain("public ICollection<QuestV2> FirstRewardQuest { get; set; } = [];");
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
        foo.SourceText.ShouldContain("public Map? MapEntity { get; set; }");
    }

    [Fact]
    public void Generator_emits_ef_core_dbcontext_with_dbset_properties()
    {
        var env = "WOW_VERSION=1.0.0.1\n";

        var mapDbd = """
COLUMNS
int ID
string Directory

BUILD 1.0.0.1
$id$ID<32>
Directory<32>
""";

        var results = RunGenerator(
            additionalFiles:
            [
                (".env", env),
                ("Map.dbd", mapDbd),
            ]);

        var context = results.Single(s => s.HintName.EndsWith("WoWDb2Context.g.cs", StringComparison.Ordinal));
        
        context.SourceText.ShouldContain("using Microsoft.EntityFrameworkCore;");
        context.SourceText.ShouldContain("public partial class WoWDb2Context : DbContext");
        context.SourceText.ShouldContain("public WoWDb2Context(DbContextOptions<WoWDb2Context> options)");
        context.SourceText.ShouldContain("public DbSet<Map> Map { get; set; } = null!;");
        context.SourceText.ShouldContain("protected override void OnModelCreating(ModelBuilder modelBuilder)");
        context.SourceText.ShouldContain("base.OnModelCreating(modelBuilder);");
        context.SourceText.ShouldContain("partial void OnModelCreatingPartial(ModelBuilder modelBuilder);");
    }

    [Fact]
    public void Generator_emits_ef_core_onmodelcreating_with_table_and_column_mappings()
    {
        var env = "WOW_VERSION=1.0.0.1\n";

        var mapDbd = """
COLUMNS
int ID
string Directory

BUILD 1.0.0.1
$id$ID<32>
Directory<32>
""";

        var results = RunGenerator(
            additionalFiles:
            [
                (".env", env),
                ("Map.dbd", mapDbd),
            ]);

        var context = results.Single(s => s.HintName.EndsWith("WoWDb2Context.g.cs", StringComparison.Ordinal));
        
        context.SourceText.ShouldContain("modelBuilder.Entity<Map>(entity =>");
        context.SourceText.ShouldContain("entity.HasKey(e => e.Id);");
    }

    [Fact]
    public void Generator_emits_ef_core_relationship_configuration()
    {
        var env = "WOW_VERSION=1.0.0.1\n";

        var mapDbd = """
COLUMNS
int ID

BUILD 1.0.0.1
$id$ID<32>
""";

        var sourceDbd = """
COLUMNS
int ID
int<Map::ID> MapID

BUILD 1.0.0.1
$id$ID<32>
MapID<u16>
""";

        var results = RunGenerator(
            additionalFiles:
            [
                (".env", env),
                ("Map.dbd", mapDbd),
                ("MapChallengeMode.dbd", sourceDbd),
            ]);

        var context = results.Single(s => s.HintName.EndsWith("WoWDb2Context.g.cs", StringComparison.Ordinal));
        
        context.SourceText.ShouldContain("entity.HasOne(e => e.Map)");
        context.SourceText.ShouldContain(".WithMany()");
        context.SourceText.ShouldContain(".HasForeignKey(e => e.MapID);");
    }

    [Fact]
    public void Generated_context_compiles_with_ef_core()
    {
        var env = "WOW_VERSION=1.0.0.1\n";

        var mapDbd = """
COLUMNS
int ID
string Directory

BUILD 1.0.0.1
$id$ID<32>
Directory<32>
""";

        var parseOptions = new CSharpParseOptions(LanguageVersion.Preview);

        // Create base compilation with necessary types
        var baseCompilation = CSharpCompilation.Create(
            assemblyName: "GeneratorCompilationTest",
            syntaxTrees: 
            [
                CSharpSyntaxTree.ParseText("""
                    namespace MimironSQL.Db2 
                    { 
                        public abstract class Db2Entity 
                        { 
                            public int Id { get; set; }
                        } 
                    }
                    """, parseOptions)
            ],
            references: GetReferences(),
            options: new CSharpCompilationOptions(OutputKind.DynamicallyLinkedLibrary));

        ImmutableArray<AdditionalText> additionalTexts =
        [
            new InMemoryAdditionalText(".env", SourceText.From(env, Encoding.UTF8)),
            new InMemoryAdditionalText("Map.dbd", SourceText.From(mapDbd, Encoding.UTF8)),
        ];

        var generator = new MimironSQL.DbContextGenerator.DbContextGenerator();
        GeneratorDriver driver = CSharpGeneratorDriver.Create(
            generators: [generator.AsSourceGenerator()],
            additionalTexts: additionalTexts,
            parseOptions: parseOptions,
            optionsProvider: new TestAnalyzerConfigOptionsProvider(ImmutableDictionary<string, string>.Empty));

        driver = driver.RunGeneratorsAndUpdateCompilation(baseCompilation, out var outputCompilation, out var diagnostics);

        var errors = outputCompilation.GetDiagnostics().Where(d => d.Severity == DiagnosticSeverity.Error).ToArray();
        errors.ShouldBeEmpty($"Generated code should compile without errors. Errors:\n{string.Join("\n", errors.Select(e => e.GetMessage()))}");
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
            MetadataReference.CreateFromFile(typeof(System.Runtime.CompilerServices.RuntimeHelpers).Assembly.Location),
            MetadataReference.CreateFromFile(typeof(System.Linq.Expressions.Expression).Assembly.Location),
        };

        // Add EF Core reference
        try
        {
            var efCoreAssemblyPath = typeof(Microsoft.EntityFrameworkCore.DbContext).Assembly.Location;
            references.Add(MetadataReference.CreateFromFile(efCoreAssemblyPath));
        }
        catch
        {
            // EF Core not available in test context
        }

        // Add System.Runtime for attributes
        var systemRuntimeAssembly = AppDomain.CurrentDomain.GetAssemblies()
            .FirstOrDefault(a => a.GetName().Name == "System.Runtime");
        if (systemRuntimeAssembly is not null)
        {
            references.Add(MetadataReference.CreateFromFile(systemRuntimeAssembly.Location));
        }

        return [.. references];
    }
}
