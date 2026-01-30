using MimironSQL.Db2;
using MimironSQL.Db2.Schema;
using MimironSQL.Formats;
using MimironSQL.Formats.Wdc5;
using MimironSQL.Providers;

using Shouldly;

namespace MimironSQL.Tests;

public sealed class Wdc5SparseInlineStringHeuristicTests
{
    [Fact]
    public void Can_open_sparse_file_and_lookup_rows_by_id()
    {
        var db2Provider = new FileSystemDb2StreamProvider(new(TestDataPaths.GetTestDataDirectory()));
        using var stream = db2Provider.OpenDb2Stream("CollectableSourceQuestSparse");
        var file = new Wdc5File(stream);
        file.Header.Flags.HasFlag(Db2Flags.Sparse).ShouldBeTrue();
        file.Header.RecordsCount.ShouldBeGreaterThan(0);
        file.TotalSectionRecordCount.ShouldBeLessThanOrEqualTo(file.Header.RecordsCount);

        var sampleIds = file.EnumerateRows()
            .Take(Math.Min(50, file.Header.RecordsCount))
            .Select(r => r.RowId)
            .Where(id => id > 0)
            .Distinct()
            .Take(10);

        sampleIds.Count().ShouldBeGreaterThan(0);

        foreach (var id in sampleIds)
        {
            file.TryGetRowById(id, out var row).ShouldBeTrue();
            row.RowId.ShouldBe(id);
        }
    }

    [Fact]
    public void Can_decode_schema_mapped_fields_in_sparse_file()
    {
        var db2Provider = new FileSystemDb2StreamProvider(new(TestDataPaths.GetTestDataDirectory()));
        using var stream = db2Provider.OpenDb2Stream("CollectableSourceQuestSparse");
        var file = new Wdc5File(stream);
        file.Header.Flags.HasFlag(Db2Flags.Sparse).ShouldBeTrue();

        var provider = new FileSystemDbdProvider(new FileSystemDbdProviderOptions(TestDataPaths.GetTestDataDirectory()));
        var mapper = new SchemaMapper(provider);
        var schema = mapper.GetSchema("CollectableSourceQuestSparse", new Db2FileLayout(file.Header.LayoutHash, file.Header.FieldsCount));

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
            row.RowId.ShouldBeGreaterThan(0);

            Should.NotThrow(() => file.ReadField<uint>(row, questId.ColumnStartIndex));

            var coords = file.ReadField<float[]>(row, pos.ColumnStartIndex);
            coords.Length.ShouldBe(3);
        }
    }
}
