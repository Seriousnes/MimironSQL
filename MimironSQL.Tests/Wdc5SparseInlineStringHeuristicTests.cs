using MimironSQL.Db2;
using MimironSQL.Db2.Wdc5;
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
    public void Can_find_inline_strings_in_sparse_file_by_heuristic()
    {
        using var stream = TestDataPaths.OpenCollectableSourceQuestSparseDb2();
        var file = new Wdc5File(stream);
        file.Header.Flags.HasFlag(Db2Flags.Sparse).ShouldBeTrue();

        var maxRowsToScan = Math.Min(file.Header.RecordsCount, 200);
        var maxFieldsToScan = Math.Min(file.Header.FieldsCount, 256);

        HashSet<string> foundStrings = new(StringComparer.Ordinal);

        foreach (var row in file.EnumerateRows().Take(maxRowsToScan))
        {
            for (var fieldIndex = 0; fieldIndex < maxFieldsToScan; fieldIndex++)
            {
                if (!row.TryGetInlineString(fieldIndex, out var value))
                    continue;

                if (value.Length > 256)
                    continue;

                if (!value.Any(char.IsLetterOrDigit))
                    continue;

                foundStrings.Add(value);

                if (foundStrings.Count >= 5)
                    break;
            }

            if (foundStrings.Count >= 5)
                break;
        }

        foundStrings.Count.ShouldBeGreaterThanOrEqualTo(5);
    }
}
