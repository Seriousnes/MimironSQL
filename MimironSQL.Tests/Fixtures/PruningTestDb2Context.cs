using MimironSQL.Db2.Model;
using MimironSQL.Db2.Query;
using MimironSQL.Providers;

namespace MimironSQL.Tests.Fixtures;

internal class PruningTestDb2Context(IDbdProvider dbdProvider, IDb2StreamProvider db2StreamProvider) : Db2Context(dbdProvider, db2StreamProvider)
{
    public Db2Table<MapWithCtor> Map { get; init; } = null!;

    protected override void OnModelCreating(Db2ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<MapWithCtor>().ToTable("Map");
    }
}