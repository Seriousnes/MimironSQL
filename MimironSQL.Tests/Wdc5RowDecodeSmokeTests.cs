using MimironSQL.Db2.Wdc5;
using Shouldly;
using System.Linq;
using Xunit;

namespace MimironSQL.Tests;

public sealed class Wdc5RowDecodeSmokeTests
{
    [Fact]
    public void Can_iterate_rows_and_decode_a_scalar_field_from_map()
    {
        var file = Wdc5File.Open(TestDataPaths.MapDb2);
        var firstRow = file.EnumerateRows().First();

        Should.NotThrow(() => firstRow.GetScalar<uint>(0));
    }

    [Fact]
    public void Virtual_id_is_available_and_allows_random_access_lookup()
    {
        var file = Wdc5File.Open(TestDataPaths.MapDb2);
        var firstRow = file.EnumerateRows().First();

        firstRow.Id.ShouldNotBe(-1);
        file.TryGetRowById(firstRow.Id, out var byId).ShouldBeTrue();
        byId.Id.ShouldBe(firstRow.Id);

        byId.GetScalar<uint>(0).ShouldBe(firstRow.GetScalar<uint>(0));
    }
}
