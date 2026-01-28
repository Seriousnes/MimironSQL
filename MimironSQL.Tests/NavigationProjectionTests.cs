using MimironSQL.Db2;
using MimironSQL.Db2.Query;
using MimironSQL.Db2.Wdc5;
using MimironSQL.Providers;
using MimironSQL.Tests.Fixtures;

using Shouldly;

namespace MimironSQL.Tests;

public sealed class NavigationProjectionTests
{
    [Fact]
    public void Scalar_navigation_projection_returns_correct_results()
    {
        var testDataDir = TestDataPaths.GetTestDataDirectory();
        var db2Provider = new FileSystemDb2StreamProvider(new(testDataDir));
        var dbdProvider = new FileSystemDbdProvider(new(testDataDir));
        var context = new TestDb2Context(dbdProvider, db2Provider);

        var names = context.MapChallengeMode
            .Where(x => x.MapID == 2441)
            .Select(x => x.Map!.MapName_lang)
            .ToList();

        names.Count.ShouldBe(2);
        names.All(n => n == "Tazavesh, the Veiled Market").ShouldBeTrue();
    }

    [Fact]
    public void Scalar_navigation_projection_avoids_n_plus_one()
    {
        var testDataDir = TestDataPaths.GetTestDataDirectory();
        var db2Provider = new FileSystemDb2StreamProvider(new(testDataDir));
        var dbdProvider = new FileSystemDbdProvider(new(testDataDir));
        var context = new TestDb2Context(dbdProvider, db2Provider);

        Wdc5FileLookupSnapshot snapshot;
        using (Wdc5FileLookupTracker.Start())
        {
            var names = context.Spell
                .Where(s => s.Id > 0)
                .Select(s => (string?)s.SpellName!.Name_lang)
                .Take(50)
                .ToList();

            names.Count.ShouldBe(50);
            snapshot = Wdc5FileLookupTracker.Snapshot();
        }

        snapshot.TotalTryGetRowByIdCalls.ShouldBe(0, "Should use batched lookup instead of N+1 TryGetRowById calls");
    }

    [Fact]
    public void Root_field_projection_avoids_entity_materialization()
    {
        var testDataDir = TestDataPaths.GetTestDataDirectory();
        var db2Provider = new FileSystemDb2StreamProvider(new(testDataDir));
        var dbdProvider = new FileSystemDbdProvider(new(testDataDir));
        var context = new PruningTestDb2Context(dbdProvider, db2Provider);

        MapWithCtor.InstancesCreated = 0;

        var names = context.Map
            .Where(x => x.Id > 0)
            .Select(x => x.Directory)
            .Take(10)
            .ToList();

        names.Count.ShouldBe(10);
        var entitiesMaterializedWithoutNavigation = MapWithCtor.InstancesCreated;

        entitiesMaterializedWithoutNavigation.ShouldBe(
            0,
            "Pruned projections without navigation should avoid entity materialization");
    }

    [Fact]
    public void Navigation_projection_reduces_field_reads()
    {
        var testDataDir = TestDataPaths.GetTestDataDirectory();
        var db2Provider = new FileSystemDb2StreamProvider(new(testDataDir));
        var dbdProvider = new FileSystemDbdProvider(new(testDataDir));
        var context = new TestDb2Context(dbdProvider, db2Provider);

        context.Spell.Schema.TryGetField("Description_lang", out var descriptionField).ShouldBeTrue();
        var spellTable = (IDb2Table)context.Spell;
        var fieldsCount = spellTable.File.Header.FieldsCount;

        Wdc5RowReadSnapshot navigationProjectionSnapshot;
        using (Wdc5RowReadTracker.Start(fieldsCount))
        {
            var names = context.Spell
                .Where(s => s.Id > 0)
                .Select(s => (string?)s.SpellName!.Name_lang)
                .Take(10)
                .ToList();

            names.Count.ShouldBe(10);
            navigationProjectionSnapshot = Wdc5RowReadTracker.Snapshot();
        }

        navigationProjectionSnapshot.StringReads[descriptionField.ColumnStartIndex].ShouldBe(
            0,
            "Root table Description_lang field should not be read when projecting only navigation fields");
    }

    [Fact]
    public void Anonymous_type_projection_with_navigation_works()
    {
        var testDataDir = TestDataPaths.GetTestDataDirectory();
        var db2Provider = new FileSystemDb2StreamProvider(new(testDataDir));
        var dbdProvider = new FileSystemDbdProvider(new(testDataDir));
        var context = new TestDb2Context(dbdProvider, db2Provider);

        var results = context.MapChallengeMode
            .Where(x => x.MapID == 2441)
            .Select(x => new { x.Id, MapName = x.Map!.MapName_lang, x.Name_lang })
            .ToList();

        results.Count.ShouldBe(2);
        results.All(r => r.Id > 0).ShouldBeTrue();
        results.All(r => r.MapName == "Tazavesh, the Veiled Market").ShouldBeTrue();
    }

    [Fact]
    public void Anonymous_type_projection_with_navigation_avoids_n_plus_one()
    {
        var testDataDir = TestDataPaths.GetTestDataDirectory();
        var db2Provider = new FileSystemDb2StreamProvider(new(testDataDir));
        var dbdProvider = new FileSystemDbdProvider(new(testDataDir));
        var context = new TestDb2Context(dbdProvider, db2Provider);

        Wdc5FileLookupSnapshot snapshot;
        using (Wdc5FileLookupTracker.Start())
        {
            var results = context.Spell
                .Where(s => s.Id > 0)
                .Select(s => new { s.Id, SpellName = (string?)s.SpellName!.Name_lang })
                .Take(50)
                .ToList();

            results.Count.ShouldBe(50);
            snapshot = Wdc5FileLookupTracker.Snapshot();
        }

        snapshot.TotalTryGetRowByIdCalls.ShouldBe(0, "Anonymous type projections should use batched lookup");
    }

    [Fact]
    public void Mixed_root_and_navigation_fields_projection_works()
    {
        var testDataDir = TestDataPaths.GetTestDataDirectory();
        var db2Provider = new FileSystemDb2StreamProvider(new(testDataDir));
        var dbdProvider = new FileSystemDbdProvider(new(testDataDir));
        var context = new TestDb2Context(dbdProvider, db2Provider);

        var results = context.Spell
            .Where(s => s.Id > 0)
            .Select(s => new { s.Description_lang, SpellName = (string?)s.SpellName!.Name_lang })
            .Take(10)
            .ToList();

        results.Count.ShouldBe(10);
        results.All(r => r.Description_lang is not null).ShouldBeTrue();
    }

    [Fact]
    public void Multiple_navigation_field_accesses_work_correctly()
    {
        var testDataDir = TestDataPaths.GetTestDataDirectory();
        var db2Provider = new FileSystemDb2StreamProvider(new(testDataDir));
        var dbdProvider = new FileSystemDbdProvider(new(testDataDir));
        var context = new TestDb2Context(dbdProvider, db2Provider);

        var results = context.Spell
            .Where(s => s.Id > 0)
            .Select(s => new
            {
                s.Id,
                SpellName = (string?)s.SpellName!.Name_lang,
                s.Description_lang
            })
            .Take(25)
            .ToList();

        results.Count.ShouldBe(25);
        results.All(r => r.Id > 0).ShouldBeTrue();
    }
}
