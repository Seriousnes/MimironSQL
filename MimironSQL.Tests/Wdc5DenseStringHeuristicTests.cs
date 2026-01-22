using MimironSQL.Db2.Wdc5;
using Shouldly;
using System;
using System.Linq;
using Xunit;

namespace MimironSQL.Tests;

public sealed class Wdc5DenseStringHeuristicTests
{
    [Fact]
    public void Can_find_at_least_one_dense_string_in_map_by_heuristic()
    {
        using var stream = TestDataPaths.OpenMapDb2();
        var file = new Wdc5File(stream);
        file.Header.Flags.HasFlag(Db2.Db2Flags.Sparse).ShouldBeFalse();
        file.Header.StringTableSize.ShouldBeGreaterThan(0);

        var maxRowsToScan = Math.Min(file.Header.RecordsCount, 200);
        var maxFieldsToScan = Math.Min(file.Header.FieldsCount, 256);

        var foundStrings = new System.Collections.Generic.HashSet<string>(StringComparer.Ordinal);
        foreach (var row in file.EnumerateRows().Take(maxRowsToScan))
        {
            for (var fieldIndex = 0; fieldIndex < maxFieldsToScan; fieldIndex++)
            {
                if (!row.TryGetDenseString(fieldIndex, out var value))
                    continue;

                if (string.IsNullOrWhiteSpace(value) || value.Length > 256)
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
