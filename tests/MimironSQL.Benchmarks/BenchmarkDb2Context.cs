using Microsoft.EntityFrameworkCore;

using MimironSQL.Benchmarks.Fixtures;

namespace MimironSQL.Benchmarks;

public sealed class BenchmarkDb2Context(DbContextOptions<BenchmarkDb2Context> options)
    : DbContext(options)
{
    public DbSet<Map> Map => Set<Map>();
    public DbSet<MapChallengeMode> MapChallengeMode => Set<MapChallengeMode>();
    public DbSet<QuestV2> QuestV2 => Set<QuestV2>();
    public DbSet<Spell> Spell => Set<Spell>();
    public DbSet<SpellName> SpellName => Set<SpellName>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        base.OnModelCreating(modelBuilder);

        modelBuilder.Entity<Map>().ToTable("Map");
        modelBuilder.Entity<MapChallengeMode>().ToTable("MapChallengeMode");
        modelBuilder.Entity<QuestV2>().ToTable("QuestV2");
        modelBuilder.Entity<Spell>().ToTable("Spell");
        modelBuilder.Entity<SpellName>().ToTable("SpellName");

        modelBuilder.Entity<MapChallengeMode>()
            .HasOne(x => x.Map)
            .WithMany(x => x.MapChallengeModes)
            .HasForeignKey(x => x.MapID);

        modelBuilder.Entity<Spell>()
            .HasOne(x => x.SpellName)
            .WithOne()
            .HasForeignKey<SpellName>(x => x.Id);
    }
}
