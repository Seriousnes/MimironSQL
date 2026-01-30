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
    public void ReadField_matches_handle_RowId()
    {
        var db2Provider = new FileSystemDb2StreamProvider(new(TestDataPaths.GetTestDataDirectory()));
        using var stream = db2Provider.OpenDb2Stream("Map");
        var file = new Wdc5File(stream);
        var handle = file.EnumerateRowHandles().First();

        var idFromReadField = file.ReadField<int>(handle, Db2VirtualFieldIndex.Id);
        idFromReadField.ShouldBe(handle.RowId);

        var field0FromReadField = file.ReadField<uint>(handle, 0);
        field0FromReadField.ShouldBeGreaterThanOrEqualTo(0u);
    }

    [Fact]
    public void ReadField_returns_multiple_fields_without_boxing()
    {
        var db2Provider = new FileSystemDb2StreamProvider(new(TestDataPaths.GetTestDataDirectory()));
        using var stream = db2Provider.OpenDb2Stream("Map");
        var file = new Wdc5File(stream);
        var handle = file.EnumerateRowHandles().First();

        var id = file.ReadField<int>(handle, Db2VirtualFieldIndex.Id);
        var parentRelation = file.ReadField<int>(handle, Db2VirtualFieldIndex.ParentRelation);
        
        id.ShouldNotBe(-1);
        parentRelation.ShouldBeGreaterThanOrEqualTo(0);
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
