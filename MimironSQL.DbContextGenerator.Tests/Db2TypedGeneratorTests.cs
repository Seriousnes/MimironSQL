using System.Collections.Immutable;
using System.Reflection;

using CASC.Net.Generators.Tests.Helpers;
using CASC.Net.Models;

using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.Text;

using Shouldly;

namespace CASC.Net.Generators.Tests;

public sealed class Db2TypedGeneratorTests
{
    [Fact]
    public void Generates_expected_files_and_compiles()
    {
        var root = "C:\\wowdbdefs";

        var manifestJson = """
        [
          { "tableName": "TestTable", "db2FileDataID": 123 }
        ]
        """;

        var dbdText = """
        COLUMNS
        int ID
        int<OtherTable::ID> OtherID
        string Name
        int Values[3]

        KEYS
        ID
        """;

        var additionalTexts = ImmutableArray.Create<AdditionalText>(
            new InMemoryAdditionalText($"{root}\\manifest.json", SourceText.From(manifestJson)),
            new InMemoryAdditionalText($"{root}\\TestTable.dbd", SourceText.From(dbdText)));

        var globalOptions = ImmutableDictionary<string, string>.Empty
            .Add("build_property.CascNetWowDbDefsRoot", root);

        var parseOptions = new CSharpParseOptions(LanguageVersion.Preview);
        var compilation = CSharpCompilation.Create(
            assemblyName: "GeneratorTests",
            syntaxTrees: [CSharpSyntaxTree.ParseText("// test", parseOptions)],
            references: GetReferences(),
            options: new CSharpCompilationOptions(OutputKind.DynamicallyLinkedLibrary));

        IIncrementalGenerator generator = new Db2TypedGenerator();

        GeneratorDriver driver = CSharpGeneratorDriver.Create(
            generators: [generator.AsSourceGenerator()],
            additionalTexts: additionalTexts,
            parseOptions: parseOptions,
            optionsProvider: new TestAnalyzerConfigOptionsProvider(globalOptions));

        driver = driver.RunGeneratorsAndUpdateCompilation(compilation, out var updatedCompilation, out var diagnostics);

        diagnostics.ShouldBeEmpty();

        var result = driver.GetRunResult();
        result.Results.ShouldHaveSingleItem();

        var generated = result.Results[0].GeneratedSources;
        generated.Any(s => s.HintName.EndsWith("WowDb2Context.g.cs", StringComparison.Ordinal)).ShouldBeTrue();
        generated.Any(s => s.HintName.EndsWith("Db2GeneratedServiceCollectionExtensions.g.cs", StringComparison.Ordinal)).ShouldBeTrue();
        generated.Any(s => s.HintName.EndsWith("Db2GeneratedReadHelpers.g.cs", StringComparison.Ordinal)).ShouldBeTrue();
        generated.Any(s => s.HintName.EndsWith("TestTable.g.cs", StringComparison.Ordinal)).ShouldBeTrue();

        var ctx = generated.Single(s => s.HintName.EndsWith("WowDb2Context.g.cs", StringComparison.Ordinal)).SourceText.ToString();
        ctx.ShouldContain("ForeignKeys");
        ctx.ShouldContain("Db2ForeignKeyMetadata");
        ctx.ShouldContain("OtherTable");

        var di = generated.Single(s => s.HintName.EndsWith("Db2GeneratedServiceCollectionExtensions.g.cs", StringComparison.Ordinal)).SourceText.ToString();
        di.ShouldContain("TryAddSingleton<WowDb2Context>");

        var emitDiagnostics = updatedCompilation.GetDiagnostics().Where(d => d.Severity == DiagnosticSeverity.Error).ToArray();
        emitDiagnostics.ShouldBeEmpty();
    }

    [Fact]
    public void Generates_multiple_tables()
    {
        var root = "C:\\wowdbdefs";

                var manifest = """
                [
                    { "tableName": "TestTable", "db2FileDataID": 123 },
                    { "tableName": "OtherTable", "db2FileDataID": 456 }
                ]
                """;

        var dbdText = """
        COLUMNS
        int ID
        string Name
        """;

        var additionalTexts = ImmutableArray.Create<AdditionalText>(
            new InMemoryAdditionalText($"{root}\\manifest.json", SourceText.From(manifest)),
            new InMemoryAdditionalText($"{root}\\TestTable.dbd", SourceText.From(dbdText)),
            new InMemoryAdditionalText($"{root}\\OtherTable.dbd", SourceText.From(dbdText)));

        var globalOptions = ImmutableDictionary<string, string>.Empty
            .Add("build_property.CascNetWowDbDefsRoot", root);

        var parseOptions = new CSharpParseOptions(LanguageVersion.Preview);
        var compilation = CSharpCompilation.Create(
            assemblyName: "GeneratorTests.Multi",
            syntaxTrees: [CSharpSyntaxTree.ParseText("// test", parseOptions)],
            references: GetReferences(),
            options: new CSharpCompilationOptions(OutputKind.DynamicallyLinkedLibrary));

        IIncrementalGenerator generator = new Db2TypedGenerator();

        GeneratorDriver driver = CSharpGeneratorDriver.Create(
            generators: [generator.AsSourceGenerator()],
            additionalTexts: additionalTexts,
            parseOptions: parseOptions,
            optionsProvider: new TestAnalyzerConfigOptionsProvider(globalOptions));

        driver = driver.RunGeneratorsAndUpdateCompilation(compilation, out var updatedCompilation, out var diagnostics);
        diagnostics.Where(d => d.Severity == DiagnosticSeverity.Error).ShouldBeEmpty();

        var result = driver.GetRunResult();
        result.Results.ShouldHaveSingleItem();

        var generated = result.Results[0].GeneratedSources;
        generated.Any(s => s.HintName.Contains("CASC.Net.TestTable", StringComparison.Ordinal)).ShouldBeTrue();
        generated.Any(s => s.HintName.Contains("CASC.Net.OtherTable", StringComparison.Ordinal)).ShouldBeTrue();

        updatedCompilation.GetDiagnostics().Where(d => d.Severity == DiagnosticSeverity.Error).ShouldBeEmpty();
    }

    private static ImmutableArray<MetadataReference> GetReferences()
    {
        var refs = new List<MetadataReference>();

        // Reference the full platform set (includes System.Runtime and friends).
        if (AppContext.GetData("TRUSTED_PLATFORM_ASSEMBLIES") is string tpa)
        {
            foreach (var path in tpa.Split(Path.PathSeparator).Select(static p => p.Trim()).Where(static p => p is { Length: > 0 }))
            {
                refs.Add(MetadataReference.CreateFromFile(path));
            }
        }

        void AddAssembly(Assembly a)
        {
            if (!string.IsNullOrWhiteSpace(a.Location))
                refs.Add(MetadataReference.CreateFromFile(a.Location));
        }

        // CASC.Net runtime + DI for generated ServiceCollection extensions
        AddAssembly(typeof(Db2Context).Assembly);
        AddAssembly(typeof(Microsoft.Extensions.DependencyInjection.IServiceCollection).Assembly);
        AddAssembly(typeof(Microsoft.Extensions.DependencyInjection.Extensions.ServiceCollectionDescriptorExtensions).Assembly);

        return refs.DistinctBy(r => r.Display ?? string.Empty).ToImmutableArray();
    }
}
