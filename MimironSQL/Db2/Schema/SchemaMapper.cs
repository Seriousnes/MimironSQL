using MimironSQL.Db2.Schema.Dbd;
using MimironSQL.Formats;
using MimironSQL.Providers;

namespace MimironSQL.Db2.Schema;

internal sealed class SchemaMapper(IDbdProvider dbdProvider)
{
    public Db2TableSchema GetSchema(string tableName, Db2FileLayout layout)
    {
        ArgumentNullException.ThrowIfNull(tableName);


        using var stream = dbdProvider.Open(tableName);
        var dbd = DbdFile.Parse(stream);

        var layoutHash = layout.LayoutHash;
        if (!dbd.TryGetLayout(layoutHash, out var dbdLayout))
            throw new InvalidDataException($"No matching LAYOUT {layoutHash:X8} found in {tableName}.dbd.");

        var expectedPhysicalCount = layout.PhysicalFieldsCount;
        if (!dbdLayout.TrySelectBuildByPhysicalColumnCount(expectedPhysicalCount, out var build, out var availableCounts))
        {
            throw new InvalidDataException(
                $"No BUILD block in {tableName}.dbd for layout {layoutHash:X8} matches physical column count {expectedPhysicalCount}. " +
                $"Available: {string.Join(", ", availableCounts)}.");
        }

        var fields = new List<Db2FieldSchema>(build.Entries.Count);
        var physicalIndex = 0;
        foreach (var entry in build.Entries)
        {
            if (entry.IsNonInline)
            {
                var virtualIndex = entry.IsId
                    ? global::MimironSQL.Db2.Db2VirtualFieldIndex.Id
                    : entry.IsRelation
                        ? global::MimironSQL.Db2.Db2VirtualFieldIndex.ParentRelation
                        : global::MimironSQL.Db2.Db2VirtualFieldIndex.UnsupportedNonInline;

                fields.Add(new Db2FieldSchema(
                    entry.Name,
                    entry.ValueType,
                    ColumnStartIndex: virtualIndex,
                    ElementCount: 0,
                    IsVerified: entry.IsVerified,
                    IsVirtual: true,
                    IsId: entry.IsId,
                    IsRelation: entry.IsRelation,
                    ReferencedTableName: entry.ReferencedTableName));
                continue;
            }

            fields.Add(new Db2FieldSchema(
                entry.Name,
                entry.ValueType,
                ColumnStartIndex: physicalIndex,
                ElementCount: entry.ElementCount,
                IsVerified: entry.IsVerified,
                IsVirtual: false,
                IsId: entry.IsId,
                IsRelation: entry.IsRelation,
                ReferencedTableName: entry.ReferencedTableName));

            physicalIndex++;
        }

        if (physicalIndex != expectedPhysicalCount)
            throw new InvalidDataException($"Resolved schema physical column count {physicalIndex} does not match DB2 physical column count {expectedPhysicalCount}.");

        return new Db2TableSchema(tableName, layoutHash, physicalIndex, fields);
    }
}
