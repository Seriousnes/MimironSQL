using System.Collections.Immutable;

namespace MimironSQL.DbContextGenerator;

internal static class TableResolver
{
    public static ImmutableArray<TableSpec> Resolve(ManifestMapping manifest, ImmutableArray<ParsedTable> parsedTables)
    {
        if (manifest.TableToDb2FileDataId.Count == 0 || parsedTables.Length == 0)
            return [];

        var builder = ImmutableArray.CreateBuilder<TableSpec>();

        foreach (var table in parsedTables.Where(t => manifest.TableToDb2FileDataId.ContainsKey(t.TableName)))
        {
            var fileDataId = manifest.TableToDb2FileDataId[table.TableName];

            builder.Add(new TableSpec(table.TableName, fileDataId, table.Columns, table.Keys, table.ForeignKeys, table.Versions));
        }

        return builder.ToImmutable();
    }
}
