using MimironSQL.Db2.Model;
using MimironSQL.Db2.Query;
using MimironSQL.Formats.Wdc5;
using MimironSQL.Providers;

namespace MimironSQL.Tests.Fixtures;

internal class TestDb2Context(IDbdProvider dbdProvider, IDb2StreamProvider db2StreamProvider) : Db2Context(dbdProvider, db2StreamProvider, Wdc5Format.Instance)
{
    public Db2Table<Map> Map { get; init; } = null!;
    public Db2Table<MapChallengeMode> MapChallengeMode { get; init; } = null!;
    public Db2Table<Spell> Spell { get; init; } = null!;
    public Db2Table<SpellName> SpellName { get; init; } = null!;
    public Db2Table<GarrType> GarrType { get; init; } = null!;
    public Db2Table<CollectableSourceQuestSparse> CollectableSourceQuestSparse { get; init; } = null!;
    public Db2Table<AccountStoreCategory> AccountStoreCategory { get; init; } = null!;

    public override void OnModelCreating(Db2ModelBuilder modelBuilder)
    {
        modelBuilder
            .Entity<Spell>()
            .HasOne(s => s.SpellName)
            .WithSharedPrimaryKey(s => s.Id, sn => sn.Id);

        modelBuilder
            .Entity<MapChallengeMode>()
            .HasMany(m => m.FirstRewardQuest)
            .WithForeignKeyArray(m => m.FirstRewardQuestID);

        modelBuilder
            .Entity<Map>()
            .HasMany(m => m.MapChallengeModes)
            .WithForeignKey(mc => mc.MapID);
    }
}
