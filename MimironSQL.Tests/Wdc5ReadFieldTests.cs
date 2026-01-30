using MimironSQL.Db2;
using MimironSQL.Formats.Wdc5;
using MimironSQL.Providers;

using Shouldly;

namespace MimironSQL.Tests;

public sealed class Wdc5ReadFieldTests
{
    [Fact]
    public void ReadField_returns_virtual_id()
    {
        var db2Provider = new FileSystemDb2StreamProvider(new(TestDataPaths.GetTestDataDirectory()));
        using var stream = db2Provider.OpenDb2Stream("Map");
        var file = new Wdc5File(stream);
        var handle = file.EnumerateRowHandles().First();

        var id = file.ReadField<int>(handle, Db2VirtualFieldIndex.Id);
        
        id.ShouldNotBe(-1);
    }

    [Fact]
    public void ReadField_returns_parent_relation()
    {
        var db2Provider = new FileSystemDb2StreamProvider(new(TestDataPaths.GetTestDataDirectory()));
        using var stream = db2Provider.OpenDb2Stream("Map");
        var file = new Wdc5File(stream);
        var handle = file.EnumerateRowHandles().First();

        var parentRelation = file.ReadField<int>(handle, Db2VirtualFieldIndex.ParentRelation);
        
        parentRelation.ShouldBeGreaterThanOrEqualTo(0);
    }

    [Fact]
    public void ReadField_returns_field_value()
    {
        var db2Provider = new FileSystemDb2StreamProvider(new(TestDataPaths.GetTestDataDirectory()));
        using var stream = db2Provider.OpenDb2Stream("Map");
        var file = new Wdc5File(stream);
        var handle = file.EnumerateRowHandles().First();

        var value = file.ReadField<uint>(handle, 0);
        
        value.ShouldBeGreaterThanOrEqualTo(0u);
    }

    [Fact]
    public void ReadField_matches_row_Get()
    {
        var db2Provider = new FileSystemDb2StreamProvider(new(TestDataPaths.GetTestDataDirectory()));
        using var stream = db2Provider.OpenDb2Stream("Map");
        var file = new Wdc5File(stream);
        var handle = file.EnumerateRowHandles().First();
        var row = file.EnumerateRows().First();

        var idFromReadField = file.ReadField<int>(handle, Db2VirtualFieldIndex.Id);
        var idFromRow = row.Get<int>(Db2VirtualFieldIndex.Id);
        
        idFromReadField.ShouldBe(idFromRow);

        var field0FromReadField = file.ReadField<uint>(handle, 0);
        var field0FromRow = row.Get<uint>(0);
        
        field0FromReadField.ShouldBe(field0FromRow);
    }

    [Fact]
    public void ReadFields_returns_multiple_fields()
    {
        var db2Provider = new FileSystemDb2StreamProvider(new(TestDataPaths.GetTestDataDirectory()));
        using var stream = db2Provider.OpenDb2Stream("Map");
        var file = new Wdc5File(stream);
        var handle = file.EnumerateRowHandles().First();

        var fieldIndices = new[] { Db2VirtualFieldIndex.Id, Db2VirtualFieldIndex.ParentRelation };
        var values = new object[2];
        
        file.ReadFields(handle, fieldIndices, values);
        
        values[0].ShouldBeOfType<int>();
        values[1].ShouldBeOfType<int>();
        ((int)values[0]).ShouldNotBe(-1);
    }

    [Fact]
    public void ReadAllFields_returns_all_fields()
    {
        var db2Provider = new FileSystemDb2StreamProvider(new(TestDataPaths.GetTestDataDirectory()));
        using var stream = db2Provider.OpenDb2Stream("Map");
        var file = new Wdc5File(stream);
        var handle = file.EnumerateRowHandles().First();

        var allValues = new object[file.Header.FieldsCount + 2];
        
        file.ReadAllFields(handle, allValues);
        
        allValues[0].ShouldBeOfType<int>();
        allValues[1].ShouldBeOfType<int>();
        ((int)allValues[0]).ShouldNotBe(-1);
    }
}
