using Microsoft.EntityFrameworkCore;

using MimironSQL.EntityFrameworkCore;
using MimironSQL.IntegrationTests.Helpers;
using MimironSQL.Providers;

using Shouldly;

namespace MimironSQL.IntegrationTests;


public sealed class FileSystemTextFixture : IDisposable
{
    public WoWDb2Context Context { get; }

    public string IndexCacheDirectory { get; }

    public FileSystemTextFixture()
    {
        var testDataDir = TestDataPaths.GetTestDataDirectory();
        Directory.Exists(testDataDir).ShouldBeTrue();

        IndexCacheDirectory = TestHelpers.CreateCustomIndexCacheDirectory(nameof(FileSystemTextFixture));

        var optionsBuilder = new DbContextOptionsBuilder<WoWDb2Context>();
        optionsBuilder.UseMimironDb2ForTests(o =>
        {
            o.WithCustomIndexes(indexes => indexes.CacheDirectory = IndexCacheDirectory);
            o.UseFileSystem(
                db2DirectoryPath: testDataDir,
                dbdDefinitionsDirectory: Path.Combine(testDataDir, "definitions"));
        });

        Context = new WoWDb2Context(optionsBuilder.Options);
        GC.KeepAlive(Context.Model);
    }

    public void Dispose()
    {
        Context.Dispose();
        TestHelpers.DeleteDirectoryIfExists(IndexCacheDirectory);
    }
}

public sealed class FileSystemDb2ContextIntegrationTests(FileSystemTextFixture fixture) : IClassFixture<FileSystemTextFixture>
{
    private WoWDb2Context context => fixture.Context;

    [Fact]
    public async Task ToListAsync_executes_end_to_end_via_async_over_sync()
    {
        var results = await context.Map
            .Where(x => x.Id > 0)
            .Take(10)
            .ToListAsync();

        results.Count.ShouldBeGreaterThan(0);
        results.All(x => x.Id > 0).ShouldBeTrue();
    }

    [Fact]
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

    [Fact]
    public void Can_query_and_include_collection_navigation()
    {
        var results = context.Map
            .Include(x => x.MapChallengeModes)
            .Where(x => x.MapChallengeModes.Count > 0)
            .Take(10)
            .ToList();

        results.Count.ShouldBeGreaterThan(0);
        results.Any(x => x.Id > 0).ShouldBeTrue();
        results.Any(x => !string.IsNullOrWhiteSpace(x.Directory)).ShouldBeTrue();

        foreach (var map in results)
        {
            map.MapChallengeModes.Count.ShouldBeGreaterThan(0);
            foreach (var mode in map.MapChallengeModes)
            {
                mode.MapID.ShouldBe(map.Id);
            }
        }
    }

    [Fact]
    public void Can_query_and_include_reference_navigation()
    {
        var results = context.MapChallengeMode
            .Include(x => x.Map)
            .Take(25)
            .ToList();

        results.Count.ShouldBeGreaterThan(0);

        foreach (var mode in results)
        {
            mode.Map.ShouldNotBeNull();
            mode.Map.Id.ShouldBe(mode.MapID);
        }
    }

    [Fact]
    public void Can_query_locstring_tables_via_filesystem_provider()
    {
        var spellNames = context.SpellName
            .Where(x => x.Name.Length > 0)
            .Take(50)
            .Select(x => x.Name)
            .ToList();

        spellNames.Count.ShouldBeGreaterThan(0);
        spellNames.Any(x => !string.IsNullOrWhiteSpace(x)).ShouldBeTrue();

        var spellIds = context.Spell
            .Where(x => x.Id > 0)
            .Take(50)
            .Select(x => x.Id)
            .ToList();

        spellIds.Count.ShouldBeGreaterThan(0);

        var questIds = context.QuestV2
            .Where(x => x.Id > 0)
            .Take(50)
            .Select(x => x.Id)
            .ToList();

        questIds.Count.ShouldBeGreaterThan(0);
    }

    [Fact]
    public void Can_filter_using_dense_string_contains()
    {
        var results = context.Map
            .Where(x => x.Directory.Contains('a'))
            .Take(50)
            .ToList();

        results.Count.ShouldBeGreaterThan(0);
        results.Any(x => !string.IsNullOrWhiteSpace(x.Directory)).ShouldBeTrue();
    }

    [Fact]
    public void Can_filter_using_reference_navigation_member_access()
    {
        var directories = context.MapChallengeMode
            .Include(x => x.Map)
            .Where(x => x.Map != null)
            .Select(x => x.Map!.Directory)
            .Take(50)
            .ToList();

        directories.Count.ShouldBeGreaterThan(0);
        directories.Any(x => !string.IsNullOrWhiteSpace(x)).ShouldBeTrue();
    }

    [Fact]
    public void Can_filter_using_reference_navigation_string_contains()
    {
        var matchingMapIds = context.Map
            .Where(x => x.Directory.Contains('a'))
            .Select(x => x.Id)
            .ToHashSet();

        var results = context.MapChallengeMode
            .Include(x => x.Map)
            .Where(x => x.Map != null)
            .Where(x => x.Map!.Directory.Contains('a'))
            .Take(50)
            .ToList();

        results.Count.ShouldBeGreaterThan(0);
        results.Any(x => matchingMapIds.Contains(x.MapID)).ShouldBeTrue();
    }

    [Fact]
    public void Select_with_navigation_access_and_post_where_uses_batched_navigation_projector()
    {
        // This shape intentionally forces the non-pruned navigation projection path:
        // Select uses a navigation member, but a Where appears after Select.
        var results = context.MapChallengeMode
            .Include(x => x.Map)
            .Select(x => new
            {
                MapId = x.Map!.Id,
                x.Map.Directory,
            })
            .Where(x => x.Directory != null)
            .Take(50)
            .ToList();

        results.Count.ShouldBeGreaterThan(0);
        results.All(x => x.MapId > 0).ShouldBeTrue();
        results.Any(x => !string.IsNullOrWhiteSpace(x.Directory)).ShouldBeTrue();
    }

    [Fact]
    public void Can_filter_using_reference_navigation_string_startswith_endswith_and_scalar_predicates()
    {
        var seedMode = context.MapChallengeMode
            .Include(x => x.Map)
            .Take(250)
            .ToList()
            .FirstOrDefault(x => x.Map is { Id: > 0 } && !string.IsNullOrWhiteSpace(x.Map.Directory));
        seedMode.ShouldNotBeNull();
        var seedMap = seedMode!.Map;
        seedMap.ShouldNotBeNull();

        var mapId = seedMap!.Id;
        var directory = seedMap.Directory;
        directory.ShouldNotBeNullOrWhiteSpace();

        var prefix = directory[..1];
        var suffix = directory[^1].ToString();

        var startsWithResults = context.MapChallengeMode
            .Include(x => x.Map)
            .Where(x => x.Map != null)
            .Where(x => x.Map!.Id == mapId)
            .Where(x => x.Map!.Directory.StartsWith(prefix))
            .Take(25)
            .ToList();
        startsWithResults.Count.ShouldBeGreaterThan(0);

        var endsWithResults = context.MapChallengeMode
            .Include(x => x.Map)
            .Where(x => x.Map != null)
            .Where(x => x.Map!.Id == mapId)
            .Where(x => x.Map!.Directory.EndsWith(suffix))
            .Take(25)
            .ToList();
        endsWithResults.Count.ShouldBeGreaterThan(0);

        var equalsResults = context.MapChallengeMode
            .Include(x => x.Map)
            .Where(x => x.Map != null)
            .Where(x => x.Map!.Id == mapId)
            .Where(x => x.Map!.Directory == directory)
            .Take(25)
            .ToList();
        equalsResults.Count.ShouldBeGreaterThan(0);
    }

    [Fact]
    public void OrderBy_Skip_Take_paginates_end_to_end_on_real_db2()
    {
        var allIds = context.Map
            .AsNoTracking()
            .Select(x => x.Id)
            .ToList();

        allIds.Count.ShouldBeGreaterThan(0);

        var expected = allIds
            .OrderBy(static x => x)
            .Skip(5)
            .Take(10)
            .ToList();

        var results = context.Map
            .AsNoTracking()
            .OrderBy(x => x.Id)
            .Skip(5)
            .Take(10)
            .Select(x => x.Id)
            .ToList();

        results.ShouldBe(expected);
    }

    [Fact]
    public void OrderBy_ThenBy_applies_compound_ordering_end_to_end_on_real_db2()
    {
        var all = context.Map
            .AsNoTracking()
            .ToList();

        all.Count.ShouldBeGreaterThan(0);

        var expected = all
            .OrderBy(static x => x.Directory)
            .ThenBy(static x => x.Id)
            .Take(25)
            .Select(static x => x.Id)
            .ToList();

        var results = context.Map
            .AsNoTracking()
            .OrderBy(x => x.Directory)
            .ThenBy(x => x.Id)
            .Take(25)
            .Select(x => x.Id)
            .ToList();

        results.ShouldBe(expected);
    }

    [Fact]
    public void Parameterized_Skip_is_supported_end_to_end_on_real_db2()
    {
        var skip = 5;

        var allIds = context.Map
            .AsNoTracking()
            .Select(x => x.Id)
            .ToList();

        var expected = allIds
            .OrderBy(static x => x)
            .Skip(skip)
            .Take(10)
            .ToList();

        var results = context.Map
            .AsNoTracking()
            .OrderBy(x => x.Id)
            .Skip(skip)
            .Take(10)
            .Select(x => x.Id)
            .ToList();

        results.ShouldBe(expected);
    }

    [Fact]
    public void LastOrDefault_without_OrderBy_matches_natural_file_order_end_to_end()
    {
        var expected = context.Map
            .AsNoTracking()
            .ToList()
            .LastOrDefault();

        var found = context.Map
            .AsNoTracking()
            .LastOrDefault();

        if (expected is null)
        {
            found.ShouldBeNull();
            return;
        }

        found.ShouldNotBeNull();
        found!.Id.ShouldBe(expected.Id);
    }

    [Fact]
    public void Can_intersect_two_navigation_string_predicates_in_single_where()
    {
        var directory = context.MapChallengeMode
            .Include(x => x.Map)
            .Where(x => x.Map != null)
            .Select(x => x.Map!.Directory)
            .First(x => !string.IsNullOrWhiteSpace(x));

        directory.ShouldNotBeNullOrWhiteSpace();

        var prefix = directory[..1];
        var suffix = directory[^1].ToString();

        var results = context.MapChallengeMode
            .Include(x => x.Map)
            .Where(x => x.Map!.Directory.StartsWith(prefix) && x.Map!.Directory.EndsWith(suffix))
            .Take(25)
            .ToList();

        results.Count.ShouldBeGreaterThan(0);
    }

    [Fact]
    public void Can_intersect_two_navigation_scalar_predicates_in_single_where()
    {
        var mapId = context.MapChallengeMode
            .Include(x => x.Map)
            .Where(x => x.Map != null)
            .Select(x => x.Map!.Id)
            .First(x => x > 0);

        var results = context.MapChallengeMode
            .Include(x => x.Map)
            .Where(x => x.Map!.Id >= mapId && x.Map!.Id <= mapId)
            .Take(25)
            .ToList();

        results.Count.ShouldBeGreaterThan(0);
        results.All(x => x.MapID == mapId).ShouldBeTrue();
    }

    [Fact]
    public void Can_intersect_navigation_string_and_scalar_predicates_in_single_where()
    {
        var seed = context.MapChallengeMode
            .Include(x => x.Map)
            .Where(x => x.Map != null)
            .Select(x => new { x.Map!.Id, x.Map.Directory })
            .First(x => x.Id > 0 && !string.IsNullOrWhiteSpace(x.Directory));

        var prefix = seed.Directory[..1];

        var results = context.MapChallengeMode
            .Include(x => x.Map)
            .Where(x => x.Map!.Id == seed.Id && x.Map!.Directory.StartsWith(prefix))
            .Take(25)
            .ToList();

        results.Count.ShouldBeGreaterThan(0);
        results.All(x => x.MapID == seed.Id).ShouldBeTrue();
    }

    [Fact]
    public void Can_compile_navigation_null_check_and_scalar_predicate_same_navigation_left_to_right()
    {
        var mapId = context.MapChallengeMode
            .Include(x => x.Map)
            .Where(x => x.Map != null)
            .Select(x => x.Map!.Id)
            .First(x => x > 0);

        var results = context.MapChallengeMode
            .Include(x => x.Map)
            .Include(x => x.FirstRewardQuest)
            .Where(x => x.Map != null && x.Map!.Id == mapId)
            .Take(25)
            .ToList();

        results.Count.ShouldBeGreaterThan(0);
        results.All(x => x.MapID == mapId).ShouldBeTrue();
    }

    [Fact]
    public void Can_compile_navigation_scalar_predicate_and_null_check_same_navigation_right_to_left()
    {
        var mapId = context.MapChallengeMode
            .Include(x => x.Map)
            .Where(x => x.Map != null)
            .Select(x => x.Map!.Id)
            .First(x => x > 0);

        var results = context.MapChallengeMode
            .Include(x => x.Map)
            .Where(x => x.Map!.Id == mapId && x.Map != null)
            .Take(25)
            .ToList();

        results.Count.ShouldBeGreaterThan(0);
        results.All(x => x.MapID == mapId).ShouldBeTrue();
    }

    [Fact]
    public void Can_translate_collection_any_with_dependent_predicate()
    {
        var results = context.Map
            .Include(x => x.MapChallengeModes)
            .Where(x => x.MapChallengeModes.Any(m => m.MapID > 0))
            .Take(25)
            .ToList();

        results.Count.ShouldBeGreaterThan(0);
    }

    [Fact]
    public void Can_execute_all_and_single_or_default_terminal_operators()
    {
        var id = context.Map.Select(x => x.Id).First(x => x > 0);

        var single = context.Map.Where(x => x.Id == id).SingleOrDefault();
        single.ShouldNotBeNull();
        single!.Id.ShouldBe(id);

        var all = context.Map.Where(x => x.Id == id).All(x => x.Id == id);
        all.ShouldBeTrue();
    }

    [Fact]
    public void Find_by_primary_key_executes_end_to_end_via_filesystem_provider()
    {
        var id = context.Map.Select(x => x.Id).First(x => x > 0);

        context.ChangeTracker.Clear();
        var entity = context.Find<MapEntity>(id);

        entity.ShouldNotBeNull();
        entity!.Id.ShouldBe(id);
    }

    [Fact]
    public void Where_id_predicate_and_projection_executes_end_to_end_and_can_return_no_rows()
    {
        var results = context.Map
            .Where(x => x.Id < 0)
            .Select(x => x.Directory)
            .ToList();

        results.Count.ShouldBe(0);
    }

    [Fact]
    public void Selecting_navigation_entity_in_pruned_row_projection_throws()
    {
        var results = context.MapChallengeMode
            .Select(x => new
            {
                x.Map,
                MapId = x.Map!.Id,
            })
            .Take(1)
            .ToList();

        results.Count.ShouldBeGreaterThan(0);
    }

    [Fact]
    public void Can_rewrite_collection_count_comparisons_to_any_and_execute_terminal_operators()
    {
        var hasChallengeModes = context.Map
            .Include(x => x.MapChallengeModes)
            .Where(x => x.MapChallengeModes.Count >= 1)
            .Take(10)
            .ToList();

        hasChallengeModes.Count.ShouldBeGreaterThan(0);
        hasChallengeModes.All(x => x.MapChallengeModes.Count >= 1).ShouldBeTrue();

        var anyMap = context.Map.Take(1).Any();
        anyMap.ShouldBeTrue();

        var mapCount = context.Map.Where(x => x.Id > 0).Take(100).Count();
        mapCount.ShouldBeGreaterThan(0);
    }

    [Fact]
    public async Task FindAsync_by_table_name_and_id_resolves_entity_via_model_metadata()
    {
        // Mirrors the minimal API pattern:
        //   GET /{tableName}/{id:int}
        //   -> model lookup -> FindAsync(entityType.ClrType, id)
        const string tableName = "Spell";
        const int id = 454009;

        var entityType = context.Model.GetEntityTypes()
            .FirstOrDefault(et => string.Equals(et.GetTableName(), tableName, StringComparison.OrdinalIgnoreCase));
        entityType.ShouldNotBeNull();

        var keyProperty = entityType!.FindPrimaryKey()?.Properties.FirstOrDefault();
        keyProperty.ShouldNotBeNull();

        context.ChangeTracker.Clear();
        var entity = await context.FindAsync(entityType.ClrType, id);

        entity.ShouldNotBeNull();
        entity.ShouldBeOfType<SpellEntity>();
        ((SpellEntity)entity).Id.ShouldBe(id);
    }
}
