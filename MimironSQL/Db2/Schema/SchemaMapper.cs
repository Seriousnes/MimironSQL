using MimironSQL.Db2.Schema.Dbd;
using MimironSQL.Db2.Wdc5;
using MimironSQL.Providers;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace MimironSQL.Db2.Schema;

public sealed class SchemaMapper(IDbdProvider dbdProvider)
{
    public Db2TableSchema GetSchema(string tableName, Wdc5File file)
    {
        ArgumentNullException.ThrowIfNull(tableName);
        ArgumentNullException.ThrowIfNull(file);

        using var stream = dbdProvider.Open(tableName);
        var dbd = DbdFile.Parse(stream);

        var layoutHash = file.Header.LayoutHash;
        if (!dbd.TryGetLayout(layoutHash, out var layout))
            throw new InvalidDataException($"No matching LAYOUT {layoutHash:X8} found in {tableName}.dbd.");

        var expectedPhysicalCount = file.Header.FieldsCount;
        if (!layout.TrySelectBuildByPhysicalColumnCount(expectedPhysicalCount, out var build, out var availableCounts))
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
                fields.Add(new Db2FieldSchema(
                    entry.Name,
                    entry.ValueType,
                    ColumnStartIndex: -1,
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
            throw new InvalidDataException($"Resolved schema physical column count {physicalIndex} does not match WDC5 header FieldsCount {expectedPhysicalCount}.");

        return new Db2TableSchema(tableName, layoutHash, physicalIndex, fields);
    }
}
