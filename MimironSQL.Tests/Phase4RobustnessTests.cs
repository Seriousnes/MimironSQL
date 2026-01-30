using MimironSQL.Db2.Query;
using MimironSQL.Db2;
using MimironSQL.Formats;
using MimironSQL.Providers;
using MimironSQL.Tests.Fixtures;

using Shouldly;

namespace MimironSQL.Tests;

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
        var mapFile = context.GetOrOpenTableRawTyped<RowHandle>(map.TableName).File;
        var parentMapIdField = map.Schema.Fields.First(f => f.Name.Equals("ParentMapID", StringComparison.OrdinalIgnoreCase));

        // Find a map with ParentMapID = 0
        var candidate = mapFile.EnumerateRows()
            .Select(r => (Id: r.RowId, ParentId: mapFile.ReadField<int>(r, parentMapIdField.ColumnStartIndex)))
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
        Should.Throw<Exception>(() =>
            _ = new MisconfiguredNavigationTestDb2Context(dbdProvider, db2Provider));
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
