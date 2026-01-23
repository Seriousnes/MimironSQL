using MimironSQL.Db2.Schema;
using MimironSQL.Db2.Wdc5;
using MimironSQL.Providers;
using Shouldly;
using System;
using System.Linq;
using Xunit;

namespace MimironSQL.Tests;

public sealed class Wdc5DenseStringHeuristicTests
{
    [Fact]
    public void Can_decode_directory_strings_in_map_using_schema()
    {
        var db2Provider = new FileSystemDb2StreamProvider(new(TestDataPaths.GetTestDataDirectory()));
        using var stream = db2Provider.OpenDb2Stream("Map");
        var file = new Wdc5File(stream);
        file.Header.Flags.HasFlag(Db2.Db2Flags.Sparse).ShouldBeFalse();
        file.Header.StringTableSize.ShouldBeGreaterThan(0);

        var provider = new FileSystemDbdProvider(new FileSystemDbdProviderOptions(TestDataPaths.GetTestDataDirectory()));
        var mapper = new SchemaMapper(provider);
        var schema = mapper.GetSchema("Map", file);
        schema.TryGetField("Directory", out var directory).ShouldBeTrue();
        directory.IsVirtual.ShouldBeFalse();
        directory.ElementCount.ShouldBe(1);

        var maxRowsToScan = Math.Min(file.Header.RecordsCount, 200);

        var foundStrings = new System.Collections.Generic.HashSet<string>(StringComparer.Ordinal);
        foreach (var row in file.EnumerateRows().Take(maxRowsToScan))
        {
            if (!row.TryGetDenseString(directory.ColumnStartIndex, out var value))
                continue;

            if (string.IsNullOrWhiteSpace(value) || value.Length > 256)
                continue;

            foundStrings.Add(value);

            if (foundStrings.Count >= 5)
                break;
        }

        foundStrings.Count.ShouldBeGreaterThanOrEqualTo(5);
    }
}
