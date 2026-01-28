using MimironSQL.Db2.Query;
using MimironSQL.Providers;
using MimironSQL.Tests.Fixtures;

using Shouldly;

namespace MimironSQL.Tests;

/// <summary>
/// Tests for Phase 4 robustness: semantics for missing rows, no silent fallbacks.
/// 
/// DOCUMENTED SEMANTICS:
/// 
/// 1. Include(...) - Left-join behavior:
///    - When FK = 0: navigation is null
///    - When related row is missing: navigation is null
///    - Never throws for missing rows (left-join semantics)
/// 
/// 2. Navigation predicates - Inner-join/semi-join semantics:
///    - Rows with missing related data are excluded from results
///    - Only rows where the navigation can be resolved and the predicate is true are included
///    - Semi-join optimization: evaluate predicate on related table first, collect matching keys
/// 
/// 3. Navigation projections - Returns null/default for missing rows:
///    - Missing related row => projected value is null/default
///    - Batched loading prevents N+1 queries
///    - Left-join semantics: missing row doesn't cause failure
/// 
/// 4. Error handling - No silent failures:
///    - Misconfiguration (table not found, schema errors) throws immediately
///    - Removed catch-and-succeed fallbacks that would silently return empty results
/// </summary>
public sealed class Phase4RobustnessTests
{
    [Fact]
    public void Include_leaves_navigation_null_when_foreign_key_is_zero()
    {
        var testDataDir = TestDataPaths.GetTestDataDirectory();
        var db2Provider = new FileSystemDb2StreamProvider(new(testDataDir));
        var dbdProvider = new FileSystemDbdProvider(new(testDataDir));
        var context = new TestDb2Context(dbdProvider, db2Provider);

        var map = context.Map;
        var parentMapIdField = map.Schema.Fields.First(f => f.Name.Equals("ParentMapID", StringComparison.OrdinalIgnoreCase));

        // Find a map with ParentMapID = 0
        var candidate = map.File.EnumerateRows()
            .Select(r => (Id: r.Id, ParentId: Convert.ToInt32(r.GetScalar<long>(parentMapIdField.ColumnStartIndex))))
            .FirstOrDefault(x => x.ParentId == 0);

        candidate.Id.ShouldBeGreaterThan(0, "Test requires a map with ParentMapID=0");

        var entity = map
            .Where(x => x.Id == candidate.Id)
            .Include(x => x.ParentMap)
            .Single();

        entity.ParentMapID.ShouldBe(0);
        entity.ParentMap.ShouldBeNull(); // Zero FK => null navigation (left-join semantics)
    }

    [Fact]
    public void Include_throws_when_referenced_table_does_not_exist()
    {
        var testDataDir = TestDataPaths.GetTestDataDirectory();
        var db2Provider = new FileSystemDb2StreamProvider(new(testDataDir));
        var dbdProvider = new FileSystemDbdProvider(new(testDataDir));

        // Creating a context with a navigation to a non-existent table should throw during model building
        var ex = Should.Throw<Exception>(() =>
            _ = new MisconfiguredNavigationTestDb2Context(dbdProvider, db2Provider));

        // Should fail during model building with proper exception, not silently
        ex.ShouldNotBeNull();
    }

    [Fact]
    public void Include_throws_when_table_file_cannot_be_opened()
    {
        var testDataDir = TestDataPaths.GetTestDataDirectory();
        
        // Create a provider that will fail when trying to open "MapChallengeMode" table during model building
        var brokenProvider = new BrokenDb2StreamProvider(testDataDir, tableName => tableName == "MapChallengeMode");
        var dbdProvider = new FileSystemDbdProvider(new(testDataDir));

        // Should throw during model building when trying to open MapChallengeMode
        var ex = Should.Throw<InvalidOperationException>(() =>
            _ = new TestDb2Context(dbdProvider, brokenProvider));

        ex.Message.ShouldContain("SimulatedFailure");
        ex.Message.ShouldContain("MapChallengeMode");
    }
}
