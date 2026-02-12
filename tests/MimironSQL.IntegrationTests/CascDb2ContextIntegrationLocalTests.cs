using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Internal;

using MimironSQL.EntityFrameworkCore;
using MimironSQL.IntegrationTests.Helpers;
using MimironSQL.Providers;

using Shouldly;

namespace MimironSQL.IntegrationTests;

public sealed class CascDb2ContextIntegrationLocalTests
{
    private WoWDb2Context context;

    public CascDb2ContextIntegrationLocalTests()
    {
        LocalEnvLocal.TryGetWowInstallRoot(out var wowInstallRoot).ShouldBeTrue();
        Directory.Exists(wowInstallRoot).ShouldBeTrue();
    
        var testDataDir = TestDataPaths.GetTestDataDirectory();
        Directory.Exists(testDataDir).ShouldBeTrue();
    
        var manifestPath = System.IO.Path.Combine(testDataDir, "manifest.json");
        File.Exists(manifestPath).ShouldBeTrue();
    
        var optionsBuilder = new DbContextOptionsBuilder<WoWDb2Context>();
        optionsBuilder.UseMimironDb2ForTests(o => o
            .UseCasc()
            .WithWowInstallRoot(wowInstallRoot)
            .WithDbdDefinitions(Path.Combine(testDataDir, "definitions"))
            .WithManifest(testDataDir, "manifest.json")
            .Apply());
    
        context = new WoWDb2Context(optionsBuilder.Options);
        _ = context.Model; // Force model creation
    }

    [LocalCascFact]
    public async Task Can_query_db2context_using_casc_db2_provider()
    {
        var results = context.Map
            .Include(x => x.MapChallengeModes)
            .Where(x => x.MapChallengeModes.Count > 0)
            .Take(10).ToList();
        results.Count.ShouldBeGreaterThan(0);
        results.Any(x => x.Id > 0).ShouldBeTrue();
        results.Any(x => !string.IsNullOrWhiteSpace(x.Directory)).ShouldBeTrue();

        var singleResults = context.Set<MapEntity>()
            .Include(x => x.MapChallengeModes)
            .Where(x => x.Id == 962)
            .ToList();
        var singleResult = singleResults.First();
        singleResult.ShouldNotBeNull();
        singleResult.Id.ShouldBe(962);
    }

    [LocalCascFact]
    public async Task Can_query_db2context_for_spell()
    {
        var result = context.Set<SpellEntity>()
            .SingleOrDefault(x => x.Id == 454009);
        result.ShouldNotBeNull();
        result.Id.ShouldBe(454009);
        result.Description.ShouldBe("""
            $?s137040[Each Maelstrom spent has a ${$s1/100}.2% chance to upgrade][Each Maelstrom Weapon spent has a ${$s2/100}.2% chance to upgrade] your next Lightning Bolt to Tempest.

            $@spelltooltip452201
            """);
    }

    [LocalCascFact]
    public async Task Can_query_by_clr_type()
    {
        const string tableName = "Spell";

        var entityType = context.Model.GetEntityTypes().FirstOrDefault(et => et.GetTableName().CompareTo($"{tableName}", StringComparison.InvariantCultureIgnoreCase) == 0);
        entityType.ShouldNotBeNull();
        var keyProperty = entityType.FindPrimaryKey()?.Properties.FirstOrDefault();
        keyProperty.ShouldNotBeNull();
        var entity = await context.FindAsync(entityType.ClrType, 454009);
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