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
        var file = Wdc5File.Open(TestDataPaths.MapDb2);
        file.Header.Flags.HasFlag(Db2.Db2Flags.Sparse).ShouldBeFalse();

        var maxRowsToScan = Math.Min(file.Header.RecordsCount, 200);
        var maxFieldsToScan = Math.Min(file.Header.FieldsCount, 256);

        var found = false;
        foreach (var row in file.EnumerateRows().Take(maxRowsToScan))
        {
            for (var fieldIndex = 0; fieldIndex < maxFieldsToScan; fieldIndex++)
            {
                if (row.TryGetDenseString(fieldIndex, out var value) && !string.IsNullOrWhiteSpace(value))
                {
                    found = true;
                    break;
                }
            }

            if (found)
                break;
        }

        found.ShouldBeTrue();
    }
}
