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
}
