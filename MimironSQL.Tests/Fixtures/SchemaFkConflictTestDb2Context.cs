using MimironSQL.Db2.Model;
using MimironSQL.Db2.Query;
using MimironSQL.Providers;

namespace MimironSQL.Tests.Fixtures;

internal sealed class SchemaFkConflictTestDb2Context(IDbdProvider dbdProvider, IDb2StreamProvider db2StreamProvider)
    : Db2Context(dbdProvider, db2StreamProvider)
{
    public Db2Table<Map> Map { get; init; } = null!;

    protected override void OnModelCreating(Db2ModelBuilder modelBuilder)
        => modelBuilder
            .Entity<Map>()
            .HasOne(m => m.ParentMap)
            .WithSharedPrimaryKey(m => m.ParentMapID, pm => pm.Id);
}
