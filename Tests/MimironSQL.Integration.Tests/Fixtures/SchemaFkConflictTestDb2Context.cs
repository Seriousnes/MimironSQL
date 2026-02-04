using MimironSQL.Db2.Model;
using MimironSQL.Db2.Query;
using MimironSQL.Formats.Wdc5;
using MimironSQL.Providers;

namespace MimironSQL.Tests.Fixtures;

internal sealed class SchemaFkConflictTestDb2Context(IDbdProvider dbdProvider, IDb2StreamProvider db2StreamProvider)
    : Db2Context(dbdProvider, db2StreamProvider, Wdc5Format.Instance)
{
    public Db2Table<Map> Map { get; init; } = null!;

    public override void OnModelCreating(Db2ModelBuilder modelBuilder)
        => modelBuilder
            .Entity<Map>()
            .HasOne(m => m.ParentMap)
            .WithSharedPrimaryKey(m => m.ParentMapID, pm => pm.Id);
}
