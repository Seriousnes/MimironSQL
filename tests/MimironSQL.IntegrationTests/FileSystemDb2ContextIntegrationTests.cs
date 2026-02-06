using MimironSQL.Db2.Query;
using MimironSQL.Formats.Wdc5;
using MimironSQL.IntegrationTests.Helpers;
using MimironSQL.Providers;

using Shouldly;

using Xunit;

namespace MimironSQL.IntegrationTests;

public sealed class FileSystemDb2ContextIntegrationTests
{
    private static WoWDb2Context CreateContext()
    {
        var testDataDir = TestDataPaths.GetTestDataDirectory();
        Directory.Exists(testDataDir).ShouldBeTrue();

        var dbdProvider = new FileSystemDbdProvider(new(testDataDir));
        var db2Provider = new FileSystemDb2StreamProvider(new(testDataDir));

        var context = new WoWDb2Context(dbdProvider, db2Provider, Wdc5Format.Instance);
        context.EnsureModelCreated();
        return context;
    }

    [Fact]
    public void Can_query_and_include_collection_navigation()
    {
        var context = CreateContext();

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
                ((int)mode.MapID).ShouldBe(map.Id);
            }
        }
    }

    [Fact]
    public void Can_query_and_include_reference_navigation()
    {
        var context = CreateContext();

        var results = context.MapChallengeMode
            .Include(x => x.Map)
            .Take(25)
            .ToList();

        results.Count.ShouldBeGreaterThan(0);

        foreach (var mode in results)
        {
            mode.Map.ShouldNotBeNull();
            mode.Map.Id.ShouldBe((int)mode.MapID);
        }
    }

    [Fact]
    public void Can_query_locstring_tables_via_filesystem_provider()
    {
        var context = CreateContext();

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
        var context = CreateContext();

        var results = context.Map
            .Where(x => x.Directory.Contains("a"))
            .Take(50)
            .ToList();

        results.Count.ShouldBeGreaterThan(0);
        results.Any(x => !string.IsNullOrWhiteSpace(x.Directory)).ShouldBeTrue();
    }

    [Fact]
    public void Can_filter_using_reference_navigation_member_access()
    {
        var context = CreateContext();

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
        var context = CreateContext();

        var matchingMapIds = context.Map
            .Where(x => x.Directory.Contains("a"))
            .Select(x => x.Id)
            .ToHashSet();

        var results = context.MapChallengeMode
            .Include(x => x.Map)
            .Where(x => x.Map != null)
            .Where(x => x.Map!.Directory.Contains("a"))
            .Take(50)
            .ToList();

        results.Count.ShouldBeGreaterThan(0);
        results.Any(x => matchingMapIds.Contains((int)x.MapID)).ShouldBeTrue();
    }

    [Fact]
    public void Select_with_navigation_access_and_post_where_uses_batched_navigation_projector()
    {
        var context = CreateContext();

        // This shape intentionally forces the non-pruned navigation projection path:
        // Select uses a navigation member, but a Where appears after Select.
        var results = context.MapChallengeMode
            .Include(x => x.Map)
            .Select(x => new
            {
                MapId = x.Map!.Id,
                Directory = x.Map.Directory,
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
        var context = CreateContext();

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
    public void Can_intersect_two_navigation_string_predicates_in_single_where()
    {
        var context = CreateContext();

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
        var context = CreateContext();

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
        results.All(x => (int)x.MapID == mapId).ShouldBeTrue();
    }

    [Fact]
    public void Can_intersect_navigation_string_and_scalar_predicates_in_single_where()
    {
        var context = CreateContext();

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
        results.All(x => (int)x.MapID == seed.Id).ShouldBeTrue();
    }

    [Fact]
    public void Can_compile_navigation_null_check_and_scalar_predicate_same_navigation_left_to_right()
    {
        var context = CreateContext();

        var mapId = context.MapChallengeMode
            .Include(x => x.Map)
            .Where(x => x.Map != null)
            .Select(x => x.Map!.Id)
            .First(x => x > 0);

        var results = context.MapChallengeMode
            .Include(x => x.Map)
            .Where(x => x.Map != null && x.Map!.Id == mapId)
            .Take(25)
            .ToList();

        results.Count.ShouldBeGreaterThan(0);
        results.All(x => (int)x.MapID == mapId).ShouldBeTrue();
    }

    [Fact]
    public void Can_compile_navigation_scalar_predicate_and_null_check_same_navigation_right_to_left()
    {
        var context = CreateContext();

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
        results.All(x => (int)x.MapID == mapId).ShouldBeTrue();
    }

    [Fact]
    public void Can_translate_collection_any_with_dependent_predicate()
    {
        var context = CreateContext();

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
        var context = CreateContext();

        var id = context.Map.Select(x => x.Id).First(x => x > 0);

        var single = context.Map.Where(x => x.Id == id).SingleOrDefault();
        single.ShouldNotBeNull();
        single!.Id.ShouldBe(id);

        var all = context.Map.Where(x => x.Id == id).All(x => x.Id == id);
        all.ShouldBeTrue();
    }

    [Fact]
    public void Selecting_navigation_entity_in_pruned_row_projection_throws()
    {
        var context = CreateContext();

        Should.Throw<NotSupportedException>(() =>
        {
            _ = context.MapChallengeMode
                .Select(x => new
                {
                    Map = x.Map,
                    MapId = x.Map!.Id,
                })
                .Take(1)
                .ToList();
        });
    }

    [Fact]
    public void Can_rewrite_collection_count_comparisons_to_any_and_execute_terminal_operators()
    {
        var context = CreateContext();

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
}
