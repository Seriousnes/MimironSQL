using Microsoft.EntityFrameworkCore;

using MimironSQL.EntityFrameworkCore;
using MimironSQL.IntegrationTests.Helpers;
using MimironSQL.Providers;

using Shouldly;

namespace MimironSQL.IntegrationTests;

public sealed class CascDb2ContextIntegrationLocalTests(CascDb2ContextIntegrationLocalTestsFixture fixture) : IClassFixture<CascDb2ContextIntegrationLocalTestsFixture>
{
    private WoWDb2Context context => fixture.Context;
   
    [LocalCascFact]
    public void Can_include_multiple_levels_of_navigation_properties()
    {
        var result = context.SpellItemEnchantment
            .Include(x => x.RequiredSkill)
            .Include(x => x.EffectArgCollection)
                .ThenInclude(x => x.SpellName)
            .SingleOrDefault(x => x.Id == 2930);

        result.ShouldNotBeNull();        
        result.RequiredSkill.ShouldNotBeNull();
        result.RequiredSkill.DisplayName.ShouldBe("Outland Enchanting");

        var spell = result.EffectArgCollection.First();
        spell.Description.ShouldBe("Instantly Kills the target.  I hope you feel good about yourself now.....");
        spell.SpellName.Name.ShouldBe("Death Touch");
    }

    [LocalCascFact(Timeout = 10000)]
    public async Task Can_query_db2context_for_spell()
    {
        var result = context.Spell
            .Include(x => x.SpellName)
            .SingleOrDefault(x => x.Id == 454009);

        result.ShouldNotBeNull();
        result.SpellName.ShouldNotBeNull();
        result.SpellName.Spell.ShouldBeSameAs(result);

        result.Id.ShouldBe(454009);
        result.Description.ShouldBe("""
            $?s137040[Each Maelstrom spent has a ${$s1/100}.2% chance to upgrade][Each Maelstrom Weapon spent has a ${$s2/100}.2% chance to upgrade] your next Lightning Bolt to Tempest.

            $@spelltooltip452201
            """);
        result.SpellName.Name.ShouldBe("Tempest");
    }

    [LocalCascFact]
    public void Can_query_by_clr_type()
    {
        const string tableName = "Spell";

        var entityType = context.Model.GetEntityTypes().FirstOrDefault(et => et.GetTableName().CompareTo($"{tableName}", StringComparison.InvariantCultureIgnoreCase) == 0);
        entityType.ShouldNotBeNull();
        var entity = context.Find(entityType.ClrType, 454009);
        entity.ShouldBeOfType<SpellEntity>();
        var spellEntity = (SpellEntity)entity;
        spellEntity.ShouldNotBeNull();
        spellEntity.Id.ShouldBe(454009);
        spellEntity.Description.ShouldBe("""
            $?s137040[Each Maelstrom spent has a ${$s1/100}.2% chance to upgrade][Each Maelstrom Weapon spent has a ${$s2/100}.2% chance to upgrade] your next Lightning Bolt to Tempest.

            $@spelltooltip452201
            """);
    }
}

public sealed class CascDb2ContextIntegrationLocalTestsFixture : IDisposable
{
    public WoWDb2Context Context;

    public string IndexCacheDirectory { get; }

    public CascDb2ContextIntegrationLocalTestsFixture()
    {
        LocalEnvLocal.TryGetWowInstallRoot(out var wowInstallRoot).ShouldBeTrue();
        Directory.Exists(wowInstallRoot).ShouldBeTrue();

        var testDataDir = TestDataPaths.GetTestDataDirectory();
        Directory.Exists(testDataDir).ShouldBeTrue();

        var manifestPath = Path.Combine(testDataDir, "manifest.json");
        File.Exists(manifestPath).ShouldBeTrue();

        var tactKeyFilePath = Path.Combine(testDataDir, "WoW.txt");
        File.Exists(tactKeyFilePath).ShouldBeTrue();

        IndexCacheDirectory = TestHelpers.CreateCustomIndexCacheDirectory(nameof(CascDb2ContextIntegrationLocalTestsFixture));

        var optionsBuilder = new DbContextOptionsBuilder<WoWDb2Context>();
        optionsBuilder.UseMimironDb2ForTests(o => o
            .WithCustomIndexes(indexes => indexes.CacheDirectory = IndexCacheDirectory)
                .UseCasc()
                .WithWowInstallRoot(wowInstallRoot)
                .WithDbdDefinitions(Path.Combine(testDataDir, "definitions"))
                .WithManifest(testDataDir, "manifest.json")
            .WithTactKeyFile(tactKeyFilePath)
                .Apply());

        Context = new WoWDb2Context(optionsBuilder.Options);
        GC.KeepAlive(Context.Model); // Force model creation
    }

    public void Dispose()
    {
        Context.Dispose();
        TestHelpers.DeleteDirectoryIfExists(IndexCacheDirectory);
    }
}