using Microsoft.EntityFrameworkCore;

namespace MimironSQL;

public class TactKeyDb2Context(DbContextOptions<TactKeyDb2Context> options) : DbContext(options)
{
    public DbSet<TactKeyEntity> TactKey
    {
        get
        {
            return field ??= Set<TactKeyEntity>();
        }
    }

    public DbSet<TactKeyLookupEntity> TactKeyLookup
    {
        get
        {
            return field ??= Set<TactKeyLookupEntity>();
        }
    }

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        base.OnModelCreating(modelBuilder);
        modelBuilder.ApplyConfigurationsFromAssembly(typeof(TactKeyDb2Context).Assembly);
    }
}
