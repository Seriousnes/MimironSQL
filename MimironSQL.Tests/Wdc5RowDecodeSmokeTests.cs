using MimironSQL.Db2.Wdc5;
using MimironSQL.Providers;

using Shouldly;

namespace MimironSQL.Tests;

public sealed class Wdc5RowDecodeSmokeTests
{
    [Fact]
    public void Can_iterate_rows_and_decode_a_scalar_field_from_map()
    {
        var db2Provider = new FileSystemDb2StreamProvider(new(TestDataPaths.GetTestDataDirectory()));
        using var stream = db2Provider.OpenDb2Stream("Map");
        var file = new Wdc5File(stream);
        var firstRow = file.EnumerateRows().First();

        Should.NotThrow(() => firstRow.GetScalar<uint>(0));
    }

    [Fact]
    public void Virtual_id_is_available_and_allows_random_access_lookup()
    {
        var db2Provider = new FileSystemDb2StreamProvider(new(TestDataPaths.GetTestDataDirectory()));
        using var stream = db2Provider.OpenDb2Stream("Map");
        var file = new Wdc5File(stream);
        var firstRow = file.EnumerateRows().First();

        firstRow.Id.ShouldNotBe(-1);
        file.TryGetRowById(firstRow.Id, out var byId).ShouldBeTrue();
        byId.Id.ShouldBe(firstRow.Id);

        byId.GetScalar<uint>(0).ShouldBe(firstRow.GetScalar<uint>(0));
    }
}
