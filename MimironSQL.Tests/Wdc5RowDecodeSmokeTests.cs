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
}
