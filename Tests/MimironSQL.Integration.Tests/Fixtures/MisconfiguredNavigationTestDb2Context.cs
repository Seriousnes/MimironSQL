using MimironSQL.Db2.Model;
using MimironSQL.Db2.Query;
using MimironSQL.Formats.Wdc5;
using MimironSQL.Providers;

namespace MimironSQL.Tests.Fixtures;

/// <summary>
/// Test context with a navigation to a non-existent table to test error handling.
/// </summary>
internal class MisconfiguredNavigationTestDb2Context(IDbdProvider dbdProvider, IDb2StreamProvider db2StreamProvider)
    : Db2Context(dbdProvider, db2StreamProvider, Wdc5Format.Instance)
{
    public Db2Table<EntityWithBrokenNavigation> TestEntity { get; init; } = null!;

    public override void OnModelCreating(Db2ModelBuilder modelBuilder)
        => modelBuilder
            .Entity<EntityWithBrokenNavigation>()
            .HasOne(e => e.NonExistentTable)
            .WithSharedPrimaryKey(e => e.Id, n => n.Id);
}

internal class EntityWithBrokenNavigation : MimironSQL.Db2.Db2Entity
{
    public NonExistentTable? NonExistentTable { get; set; }
}

internal class NonExistentTable : MimironSQL.Db2.Db2Entity
{
    public string SomeField { get; set; } = string.Empty;
}
