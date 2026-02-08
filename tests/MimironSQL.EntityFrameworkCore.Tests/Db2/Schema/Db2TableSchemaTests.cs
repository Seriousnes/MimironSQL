using MimironSQL.Db2;
using MimironSQL.EntityFrameworkCore.Db2.Schema;

using Shouldly;

namespace MimironSQL.EntityFrameworkCore.Tests;

public sealed class Db2TableSchemaTests
{
    [Fact]
    public void Db2TableSchema_TryGetField_ResolvesByExactName()
    {
        var fields = new[]
        {
            new Db2FieldSchema("ID", Db2ValueType.Int64, ColumnStartIndex: 0, ElementCount: 1, IsVerified: true, IsVirtual: false, IsId: true, IsRelation: false, ReferencedTableName: null),
            new Db2FieldSchema("Name", Db2ValueType.String, ColumnStartIndex: 1, ElementCount: 1, IsVerified: true, IsVirtual: false, IsId: false, IsRelation: false, ReferencedTableName: null),
        };

        var schema = new Db2TableSchema("Foo", layoutHash: 0x12345678, physicalColumnCount: 2, fields);

        schema.TryGetField("Name", out var f).ShouldBeTrue();
        f.Name.ShouldBe("Name");

        schema.TryGetField("Missing", out _).ShouldBeFalse();
    }

    [Fact]
    public void Db2TableSchema_TryGetFieldCaseInsensitive_FallsBackToOrdinalIgnoreCase()
    {
        var fields = new[]
        {
            new Db2FieldSchema("DisplayID", Db2ValueType.Int64, ColumnStartIndex: 0, ElementCount: 1, IsVerified: true, IsVirtual: false, IsId: false, IsRelation: false, ReferencedTableName: null),
        };

        var schema = new Db2TableSchema("Foo", layoutHash: 0, physicalColumnCount: 1, fields);

        schema.TryGetFieldCaseInsensitive("displayid", out var f).ShouldBeTrue();
        f.Name.ShouldBe("DisplayID");

        schema.TryGetFieldCaseInsensitive("nope", out _).ShouldBeFalse();
    }
}
