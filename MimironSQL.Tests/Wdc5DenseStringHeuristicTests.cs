using MimironSQL.Db2.Schema;
using MimironSQL.Db2;
using MimironSQL.Formats;
using MimironSQL.Formats.Wdc5;
using MimironSQL.Providers;

using Shouldly;

namespace MimironSQL.Tests;

public sealed class Wdc5DenseStringHeuristicTests
{
    [Fact]
    public void Can_decode_directory_strings_in_map_using_schema()
    {
        var db2Provider = new FileSystemDb2StreamProvider(new(TestDataPaths.GetTestDataDirectory()));
        using var stream = db2Provider.OpenDb2Stream("Map");
        var file = new Wdc5File(stream);
        file.Header.Flags.HasFlag(Db2Flags.Sparse).ShouldBeFalse();
        file.Header.StringTableSize.ShouldBeGreaterThan(0);

        var provider = new FileSystemDbdProvider(new FileSystemDbdProviderOptions(TestDataPaths.GetTestDataDirectory()));
        var mapper = new SchemaMapper(provider);
        var schema = mapper.GetSchema("Map", new Db2FileLayout(file.Header.LayoutHash, file.Header.FieldsCount));
        schema.TryGetField("Directory", out var directory).ShouldBeTrue();
        directory.IsVirtual.ShouldBeFalse();
        directory.ElementCount.ShouldBe(1);

        var maxRowsToScan = Math.Min(file.Header.RecordsCount, 200);

        var foundStrings = new HashSet<string>(StringComparer.Ordinal);
        foreach (var row in file.EnumerateRows().Take(maxRowsToScan))
        {
            string value;
            try
            {
                value = row.Get<string>(directory.ColumnStartIndex);
            }
            catch
            {
                continue;
            }

            if (string.IsNullOrWhiteSpace(value) || value.Length > 256)
                continue;

            foundStrings.Add(value);

            if (foundStrings.Count >= 5)
                break;
        }

        foundStrings.Count.ShouldBeGreaterThanOrEqualTo(5);
    }
}
