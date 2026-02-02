using System.Collections.Immutable;
using System.Text;

using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.Text;

namespace CASC.Net.Generators;

[Generator(LanguageNames.CSharp)]
public sealed class Db2TypedGenerator : IIncrementalGenerator
{
    private static readonly DiagnosticDescriptor DuplicateTableSchema = new(
        id: "CASCNETDB2GEN003",
        title: "Duplicate table schema",
        messageFormat: "CASC.Net.Generators: multiple .dbd definitions were found for table '{0}'. Using the first one.",
        category: "CASC.Net.Generators",
        defaultSeverity: DiagnosticSeverity.Warning,
        isEnabledByDefault: true);

    private static readonly DiagnosticDescriptor CollidingSafeTableName = new(
        id: "CASCNETDB2GEN004",
        title: "Table name collision after sanitization",
        messageFormat: "CASC.Net.Generators: multiple tables map to the same generated identifier '{0}' (e.g., '{1}' and '{2}'). Using '{3}' and skipping the others.",
        category: "CASC.Net.Generators",
        defaultSeverity: DiagnosticSeverity.Warning,
        isEnabledByDefault: true);

    public void Initialize(IncrementalGeneratorInitializationContext context)
    {
        var manifests = context.AdditionalTextsProvider
            .Where(static at => at.Path.EndsWith("manifest.json", StringComparison.OrdinalIgnoreCase))
            .Select(static (at, ct) => (path: at.Path, text: at.GetText(ct)))
            .Select(static (tuple, _) => new ManifestFile(tuple.path, ManifestParser.Parse(tuple.text)))
            .Collect();

        var tables = context.AdditionalTextsProvider
            .Where(static at => at.Path.EndsWith(".dbd", StringComparison.OrdinalIgnoreCase))
            .Select(static (at, ct) => (path: at.Path, tableName: Path.GetFileNameWithoutExtension(at.Path), text: at.GetText(ct)))
            .Select(static (tuple, _) => (tuple.path, table: DbdParser.TryParse(tuple.tableName, tuple.text)))
            .Where(static t => t.table is not null)
            .Select(static (t, _) => new DbdFile(t.path, t.table!))
            .Collect();

        context.RegisterSourceOutput(manifests.Combine(tables), static (spc, input) =>
        {
            var (manifestFiles, dbdFiles) = input;

            // Single-schema generation: WoWDBDefs does not distinguish by flavor.
            var manifest = manifestFiles
                .Select(static mf => mf.Mapping)
                .FirstOrDefault(static m => m.TableToDb2FileDataId.Count > 0);

            if (manifest is null)
                return;

            var uniqueTables = new Dictionary<string, ParsedTable>(StringComparer.OrdinalIgnoreCase);
            foreach (var dbd in dbdFiles)
            {
                if (uniqueTables.ContainsKey(dbd.Table.TableName))
                {
                    spc.ReportDiagnostic(Diagnostic.Create(DuplicateTableSchema, Location.None, dbd.Table.TableName));
                    continue;
                }

                uniqueTables[dbd.Table.TableName] = dbd.Table;
            }

            var parsedTables = uniqueTables.Values.ToImmutableArray();
            var resolvedTables = TableResolver.Resolve(manifest, parsedTables);

            // Some WoWDBDefs table names collide after identifier normalization (e.g. Item-sparse vs ItemSparse).
            // We must ensure unique hint names and unique generated type namespaces.
            var uniqueBySafeName = new Dictionary<string, TableSpec>(StringComparer.OrdinalIgnoreCase);
            foreach (var table in resolvedTables)
            {
                var safeTableName = IdentifierHelper.ToSafeTypeName(table.TableName);
                if (!uniqueBySafeName.TryGetValue(safeTableName, out var existing))
                {
                    uniqueBySafeName.Add(safeTableName, table);
                    continue;
                }

                static int Score(TableSpec t, string safe)
                    => string.Equals(t.TableName, safe, StringComparison.OrdinalIgnoreCase) ? 2 : 1;

                var existingScore = Score(existing, safeTableName);
                var newScore = Score(table, safeTableName);

                TableSpec chosen;
                TableSpec skipped;

                if (newScore > existingScore)
                {
                    chosen = table;
                    skipped = existing;
                }
                else if (newScore < existingScore)
                {
                    chosen = existing;
                    skipped = table;
                }
                else
                {
                    // Tie-break deterministically.
                    if (string.Compare(existing.TableName, table.TableName, StringComparison.OrdinalIgnoreCase) <= 0)
                    {
                        chosen = existing;
                        skipped = table;
                    }
                    else
                    {
                        chosen = table;
                        skipped = existing;
                    }
                }

                if (!ReferenceEquals(uniqueBySafeName[safeTableName], chosen))
                    uniqueBySafeName[safeTableName] = chosen;

                spc.ReportDiagnostic(Diagnostic.Create(
                    CollidingSafeTableName,
                    Location.None,
                    safeTableName,
                    existing.TableName,
                    table.TableName,
                    chosen.TableName));
            }

            resolvedTables = uniqueBySafeName.Values
                .OrderBy(static t => IdentifierHelper.ToSafeTypeName(t.TableName), StringComparer.OrdinalIgnoreCase)
                .ThenBy(static t => t.TableName, StringComparer.OrdinalIgnoreCase)
                .ToImmutableArray();

            var ns = GeneratorConstants.RootNamespace;

            spc.AddSource($"{ns}.Db2GeneratedReadHelpers.g.cs", SourceText.From(GeneratedCodeRenderer.RenderReadHelpers(ns), Encoding.UTF8));

            foreach (var table in resolvedTables)
            {
                var safeTableName = IdentifierHelper.ToSafeTypeName(table.TableName);
                var hint = $"{ns}.{safeTableName}.g.cs";
                spc.AddSource(hint, SourceText.From(GeneratedCodeRenderer.RenderRow(ns, table), Encoding.UTF8));
            }

            spc.AddSource($"{ns}.WowDb2Context.g.cs", SourceText.From(GeneratedCodeRenderer.RenderContext(ns, resolvedTables), Encoding.UTF8));
            spc.AddSource($"{ns}.Db2GeneratedServiceCollectionExtensions.g.cs", SourceText.From(GeneratedCodeRenderer.RenderServiceCollectionExtensions(ns, resolvedTables), Encoding.UTF8));
        });
    }

    private sealed record ManifestFile(string Path, ManifestMapping Mapping);

    private sealed record DbdFile(string Path, ParsedTable Table);
}
