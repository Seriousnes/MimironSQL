using MimironSQL.Db2.Model;
using MimironSQL.Db2.Query;
using MimironSQL.Providers;

namespace MimironSQL.Tests.Fixtures;

/// <summary>
/// Test context with a navigation to a non-existent table to test error handling.
/// </summary>
internal class MisconfiguredNavigationTestDb2Context(IDbdProvider dbdProvider, IDb2StreamProvider db2StreamProvider)
    : Db2Context(dbdProvider, db2StreamProvider)
{
    public Db2Table<EntityWithBrokenNavigation> TestEntity { get; init; } = null!;

    protected override void OnModelCreating(Db2ModelBuilder modelBuilder)
        => modelBuilder
            .Entity<EntityWithBrokenNavigation>()
            .HasOne(e => e.NonExistentTable)
            .WithSharedPrimaryKey(e => e.Id, n => n.Id);
}

internal class EntityWithBrokenNavigation : MimironSQL.Db2.Wdc5Entity
{
    public NonExistentTable? NonExistentTable { get; set; }
}

internal class NonExistentTable : MimironSQL.Db2.Wdc5Entity
{
    public string SomeField { get; set; } = string.Empty;
}
