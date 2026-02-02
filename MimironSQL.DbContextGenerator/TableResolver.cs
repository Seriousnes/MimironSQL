using System.Collections.Immutable;

namespace CASC.Net.Generators;

internal static class TableResolver
{
    public static ImmutableArray<TableSpec> Resolve(ManifestMapping manifest, ImmutableArray<ParsedTable> parsedTables)
    {
        if (manifest.TableToDb2FileDataId.Count == 0 || parsedTables.Length == 0)
            return [];

        var builder = ImmutableArray.CreateBuilder<TableSpec>();

        foreach (var table in parsedTables)
        {
            if (table is null || !manifest.TableToDb2FileDataId.TryGetValue(table.TableName, out var fileDataId))
                continue;

            builder.Add(new TableSpec(table.TableName, fileDataId, table.Columns, table.Keys, table.ForeignKeys, table.Versions));
        }

        return builder.ToImmutable();
    }
}
