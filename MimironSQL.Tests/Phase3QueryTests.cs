using MimironSQL.Db2.Query;
using MimironSQL.Db2.Schema;
using MimironSQL.Db2.Wdc5;
using MimironSQL.Providers;
using Shouldly;
using System;
using System.Linq;
using Xunit;

namespace MimironSQL.Tests;

public sealed class Phase3QueryTests
{
    [Fact]
    public void Can_query_dense_table_with_string_filter_and_take()
    {
        var testDataDir = TestDataPaths.GetTestDataDirectory();
        var db2Provider = new FileSystemDb2StreamProvider(new(testDataDir));
        var dbdProvider = new FileSystemDbdProvider(new(testDataDir));
        var context = new TestDb2Context(dbdProvider, db2Provider);

        var map = context.Map;

        var results = map
            .Where(x => x.Directory.Contains("o"))
            .Take(25)
            .ToArray();

        results.Length.ShouldBeGreaterThan(0);
        results.Any(x => x.Id > 0).ShouldBeTrue();
    }

    [Fact]
    public void Can_query_dense_table_with_startswith_and_endswith()
    {
        var testDataDir = TestDataPaths.GetTestDataDirectory();
        var db2Provider = new FileSystemDb2StreamProvider(new(testDataDir));
        var dbdProvider = new FileSystemDbdProvider(new(testDataDir));
        var context = new TestDb2Context(dbdProvider, db2Provider);

        using var stream = db2Provider.OpenDb2Stream("Map");
        var file = new Wdc5File(stream);
        var schema = new SchemaMapper(dbdProvider).GetSchema("Map", file);
        schema.TryGetField("Directory", out var directoryField).ShouldBeTrue();

        var sample = file.EnumerateRows()
            .Take(Math.Min(200, file.Header.RecordsCount))
            .Select(r => r.TryGetString(directoryField.ColumnStartIndex, out var s) ? s : string.Empty)
            .FirstOrDefault(s => !string.IsNullOrWhiteSpace(s));

        sample.ShouldNotBeNull();
        sample.Length.ShouldBeGreaterThanOrEqualTo(2);

        var prefix = sample[..2];
        var suffix = sample[^2..];

        context.Map.Where(x => x.Directory.StartsWith(prefix)).Take(10).ToArray().Length.ShouldBeGreaterThan(0);
        context.Map.Where(x => x.Directory.EndsWith(suffix)).Take(10).ToArray().Length.ShouldBeGreaterThan(0);
    }

    [Fact]
    public void Can_query_spell_names_with_string_contains_and_project_ids()
    {
        var testDataDir = TestDataPaths.GetTestDataDirectory();
        var db2Provider = new FileSystemDb2StreamProvider(new(testDataDir));
        var dbdProvider = new FileSystemDbdProvider(new(testDataDir));
        var context = new TestDb2Context(dbdProvider, db2Provider);

        var spells = context.Spell;

        var ids = spells
            .Where(s => s.Id > 0)
            .Select(s => s.Id)
            .Take(20)
            .ToArray();

        ids.Length.ShouldBeGreaterThanOrEqualTo(1);
        ids.All(id => id > 0).ShouldBeTrue();
    }

    [Fact]
    public void Can_query_sparse_table_using_virtual_id_and_virtual_relation_fields()
    {
        var testDataDir = TestDataPaths.GetTestDataDirectory();
        var db2Provider = new FileSystemDb2StreamProvider(new(testDataDir));
        var dbdProvider = new FileSystemDbdProvider(new(testDataDir));
        var context = new TestDb2Context(dbdProvider, db2Provider);

        var table = context.CollectableSourceQuestSparse;

        var results = table
            .Where(x => x.CollectableSourceInfoID > 0)
            .Take(50)
            .ToArray();

        results.Length.ShouldBeGreaterThan(0);
        results.All(r => r.Id > 0).ShouldBeTrue();
        results.All(r => r.CollectableSourceInfoID > 0).ShouldBeTrue();
        results.All(r => r.QuestID >= 0).ShouldBeTrue();
    }

    [Fact]
    public void Can_use_first_or_default_on_a_filtered_query()
    {
        var testDataDir = TestDataPaths.GetTestDataDirectory();
        var db2Provider = new FileSystemDb2StreamProvider(new(testDataDir));
        var dbdProvider = new FileSystemDbdProvider(new(testDataDir));
        var context = new TestDb2Context(dbdProvider, db2Provider);

        var categories = context.AccountStoreCategory;

        var first = categories
            .Where(x => x.StoreFrontID > 0)
            .FirstOrDefault();

        first.ShouldNotBeNull();
        first!.Id.ShouldBeGreaterThan(0);
    }

    [Fact]
    public void Can_auto_open_tables_by_name_and_use_any_count_single()
    {
        var testDataDir = TestDataPaths.GetTestDataDirectory();

        var db2Provider = new FileSystemDb2StreamProvider(new(testDataDir));
        var dbdProvider = new FileSystemDbdProvider(new(testDataDir));
        var context = new TestDb2Context(dbdProvider, db2Provider);

        var map = context.Map;

        map.Schema.TableName.ShouldBe("Map");

        map.Any().ShouldBeTrue();
        map.Count().ShouldBeGreaterThan(0);

        var single = map.Where(x => x.Id == x.Id).Take(1).Single();
        single.ShouldNotBeNull();
    }

    private sealed class Map
    {
        public int Id { get; set; }
        public string Directory { get; set; } = string.Empty;
        public string MapName_lang { get; set; } = string.Empty;
    }

    private sealed class Spell
    {
        public int Id { get; set; }
    }

    private sealed class CollectableSourceQuestSparse
    {
        public int Id { get; set; }
        public int QuestID { get; set; }
        public int CollectableSourceInfoID { get; set; }
    }

    private sealed class AccountStoreCategory
    {
        public int Id { get; set; }
        public int StoreFrontID { get; set; }
        public string Name_lang { get; set; } = string.Empty;
    }

    private sealed class TestDb2Context(IDbdProvider dbdProvider, IDb2StreamProvider db2StreamProvider) : Db2Context(dbdProvider, db2StreamProvider)
    {
        public Db2Table<Map> Map { get; init; } = null!;
        public Db2Table<Spell> Spell { get; init; } = null!;
        public Db2Table<CollectableSourceQuestSparse> CollectableSourceQuestSparse { get; init; } = null!;
        public Db2Table<AccountStoreCategory> AccountStoreCategory { get; init; } = null!;
    }
}
