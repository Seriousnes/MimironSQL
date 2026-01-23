using MimironSQL.Db2;
using MimironSQL.Db2.Schema;
using MimironSQL.Db2.Wdc5;
using MimironSQL.Providers;
using Shouldly;
using System;
using System.Collections.Generic;
using System.Linq;
using Xunit;

namespace MimironSQL.Tests;

public sealed class Wdc5SparseInlineStringHeuristicTests
{
    [Fact]
    public void Can_open_sparse_file_and_lookup_rows_by_id()
    {
        using var stream = TestDataPaths.OpenCollectableSourceQuestSparseDb2();
        var file = new Wdc5File(stream);
        file.Header.Flags.HasFlag(Db2Flags.Sparse).ShouldBeTrue();
        file.Header.RecordsCount.ShouldBeGreaterThan(0);
        file.TotalSectionRecordCount.ShouldBeLessThanOrEqualTo(file.Header.RecordsCount);

        var sampleIds = file.EnumerateRows()
            .Take(Math.Min(50, file.Header.RecordsCount))
            .Select(r => r.Id)
            .Where(id => id > 0)
            .Distinct()
            .Take(10)
            .ToArray();

        sampleIds.Length.ShouldBeGreaterThan(0);

        foreach (var id in sampleIds)
        {
            file.TryGetRowById(id, out var row).ShouldBeTrue();
            row.Id.ShouldBe(id);
        }
    }

    [Fact]
    public void Can_decode_schema_mapped_fields_in_sparse_file()
    {
        using var stream = TestDataPaths.OpenCollectableSourceQuestSparseDb2();
        var file = new Wdc5File(stream);
        file.Header.Flags.HasFlag(Db2Flags.Sparse).ShouldBeTrue();

        var provider = new FileSystemDbdProvider(new FileSystemDbdProviderOptions(TestDataPaths.GetTestDataDirectory()));
        var mapper = new SchemaMapper(provider);
        var schema = mapper.GetSchema("CollectableSourceQuestSparse", file);

        schema.TryGetField("ID", out var idField).ShouldBeTrue();
        idField.IsVirtual.ShouldBeTrue();
        idField.IsId.ShouldBeTrue();

        schema.TryGetField("CollectableSourceInfoID", out var relation).ShouldBeTrue();
        relation.IsVirtual.ShouldBeTrue();
        relation.IsId.ShouldBeFalse();

        schema.TryGetField("QuestID", out var questId).ShouldBeTrue();
        questId.IsVirtual.ShouldBeFalse();
        questId.ElementCount.ShouldBe(1);

        schema.TryGetField("QuestPosition", out var pos).ShouldBeTrue();
        pos.IsVirtual.ShouldBeFalse();
        pos.ElementCount.ShouldBe(3);

        var maxRowsToScan = Math.Min(file.Header.RecordsCount, 100);

        foreach (var row in file.EnumerateRows().Take(maxRowsToScan))
        {
            row.Id.ShouldBeGreaterThan(0);

            Should.NotThrow(() => row.GetScalar<uint>(questId.ColumnStartIndex));

            var coords = row.GetArray<float>(pos.ColumnStartIndex);
            coords.Length.ShouldBe(3);
        }
    }
}
