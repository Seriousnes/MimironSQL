using System.ComponentModel.DataAnnotations.Schema;

using MimironSQL.Db2;
using MimironSQL.Db2.Model;
using MimironSQL.Db2.Query;
using MimironSQL.Formats;
using MimironSQL.Providers;

using Shouldly;

namespace MimironSQL.Tests;

public sealed class ForeignKeyAttributeNavigationTests
{
    [Fact]
    public void ForeignKeyAttribute_on_reference_navigation_configures_fk_navigation()
    {
        var testDataDir = TestDataPaths.GetTestDataDirectory();
        var db2Provider = new FileSystemDb2StreamProvider(new(testDataDir));
        var dbdProvider = new FileSystemDbdProvider(new(testDataDir));
        var context = new MapForeignKeyAttributeOnNavigationContext(dbdProvider, db2Provider);

        var map = context.Maps;
        var parentMapIdField = map.Schema.Fields.First(f => f.Name.Equals("ParentMapID", StringComparison.OrdinalIgnoreCase));

        var mapFile = context.GetOrOpenTableRawTyped<RowHandle>(map.TableName).File;

        var (Id, ParentId) = mapFile.EnumerateRows()
            .Select(r => (Id: r.RowId, ParentId: mapFile.ReadField<int>(r, parentMapIdField.ColumnStartIndex)))
            .FirstOrDefault(x => x.ParentId > 0 && mapFile.TryGetRowById(x.ParentId, out _));

        ParentId.ShouldBeGreaterThan(0);

        var entity = map
            .Where(x => x.Id == Id)
            .Include(x => x.ParentMap)
            .Single();

        entity.ParentMapID.ShouldBe(ParentId);
        entity.ParentMap.ShouldNotBeNull();
        entity.ParentMap!.Id.ShouldBe(ParentId);
    }

    [Fact]
    public void ForeignKeyAttribute_on_fk_property_configures_fk_navigation()
    {
        var testDataDir = TestDataPaths.GetTestDataDirectory();
        var db2Provider = new FileSystemDb2StreamProvider(new(testDataDir));
        var dbdProvider = new FileSystemDbdProvider(new(testDataDir));
        var context = new MapForeignKeyAttributeOnFkContext(dbdProvider, db2Provider);

        var map = context.Maps;
        var parentMapIdField = map.Schema.Fields.First(f => f.Name.Equals("ParentMapID", StringComparison.OrdinalIgnoreCase));

        var mapFile = context.GetOrOpenTableRawTyped<RowHandle>(map.TableName).File;

        var (Id, ParentId) = mapFile.EnumerateRows()
            .Select(r => (Id: r.RowId, ParentId: mapFile.ReadField<int>(r, parentMapIdField.ColumnStartIndex)))
            .FirstOrDefault(x => x.ParentId > 0 && mapFile.TryGetRowById(x.ParentId, out _));

        ParentId.ShouldBeGreaterThan(0);

        var entity = map
            .Where(x => x.Id == Id)
            .Include(x => x.ParentMap)
            .Single();

        entity.ParentMapID.ShouldBe(ParentId);
        entity.ParentMap.ShouldNotBeNull();
        entity.ParentMap!.Id.ShouldBe(ParentId);
    }

    [Fact]
    public void Fluent_navigation_configuration_conflicts_with_ForeignKeyAttribute()
    {
        var testDataDir = TestDataPaths.GetTestDataDirectory();
        var db2Provider = new FileSystemDb2StreamProvider(new(testDataDir));
        var dbdProvider = new FileSystemDbdProvider(new(testDataDir));

        var ex = Should.Throw<NotSupportedException>(() => _ = new MapForeignKeyFluentConflictContext(dbdProvider, db2Provider));
        ex.Message.ShouldContain("[ForeignKey]");
        ex.Message.ShouldContain(nameof(Map_ForeignKeyOnNavigation.ParentMap));
    }
}

[Table("Map")]
internal sealed class Map_ForeignKeyOnNavigation : Db2Entity
{
    public int ParentMapID { get; set; }

    [ForeignKey(nameof(ParentMapID))]
    public Map_ForeignKeyOnNavigation? ParentMap { get; set; }
}

internal sealed class MapForeignKeyAttributeOnNavigationContext(IDbdProvider dbdProvider, IDb2StreamProvider db2StreamProvider)
    : Db2Context(dbdProvider, db2StreamProvider)
{
    public Db2Table<Map_ForeignKeyOnNavigation> Maps { get; init; } = null!;
}

[Table("Map")]
internal sealed class Map_ForeignKeyOnFk : Db2Entity
{
    [ForeignKey(nameof(ParentMap))]
    public int ParentMapID { get; set; }

    public Map_ForeignKeyOnFk? ParentMap { get; set; }
}

internal sealed class MapForeignKeyAttributeOnFkContext(IDbdProvider dbdProvider, IDb2StreamProvider db2StreamProvider)
    : Db2Context(dbdProvider, db2StreamProvider)
{
    public Db2Table<Map_ForeignKeyOnFk> Maps { get; init; } = null!;
}

[Table("Map")]
internal sealed class Map_ForeignKeyOnNavigation_FluentConflict : Db2Entity
{
    public int ParentMapID { get; set; }

    [ForeignKey(nameof(ParentMapID))]
    public Map_ForeignKeyOnNavigation_FluentConflict? ParentMap { get; set; }
}

internal sealed class MapForeignKeyFluentConflictContext(IDbdProvider dbdProvider, IDb2StreamProvider db2StreamProvider)
    : Db2Context(dbdProvider, db2StreamProvider)
{
    public Db2Table<Map_ForeignKeyOnNavigation_FluentConflict> Maps { get; init; } = null!;

    protected override void OnModelCreating(Db2ModelBuilder modelBuilder)
        => modelBuilder
            .Entity<Map_ForeignKeyOnNavigation_FluentConflict>()
            .HasOne(m => m.ParentMap)
            .WithForeignKey(m => m.ParentMapID);
}
