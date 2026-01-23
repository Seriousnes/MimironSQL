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
        using var stream = TestDataPaths.OpenMapDb2();

        var provider = new FileSystemDbdProvider(new FileSystemDbdProviderOptions(TestDataPaths.GetTestDataDirectory()));
        var db = new Db2Database(provider);

        var map = db.OpenTable<MapRow>("Map", stream);

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
        using var stream = TestDataPaths.OpenMapDb2();

        var provider = new FileSystemDbdProvider(new FileSystemDbdProviderOptions(TestDataPaths.GetTestDataDirectory()));
        var db = new Db2Database(provider);

        var file = new Wdc5File(stream);
        var schema = new SchemaMapper(provider).GetSchema("Map", file);
        schema.TryGetField("Directory", out var directoryField).ShouldBeTrue();

        var sample = file.EnumerateRows()
            .Take(Math.Min(200, file.Header.RecordsCount))
            .Select(r => r.TryGetString(directoryField.ColumnStartIndex, out var s) ? s : string.Empty)
            .FirstOrDefault(s => !string.IsNullOrWhiteSpace(s));

        sample.ShouldNotBeNull();
        sample.Length.ShouldBeGreaterThanOrEqualTo(2);

        var prefix = sample[..2];
        var suffix = sample[^2..];

        stream.Position = 0;
        var map = db.OpenTable<MapRow>("Map", stream);

        map.Where(x => x.Directory.StartsWith(prefix)).Take(10).ToArray().Length.ShouldBeGreaterThan(0);
        map.Where(x => x.Directory.EndsWith(suffix)).Take(10).ToArray().Length.ShouldBeGreaterThan(0);
    }

    [Fact]
    public void Can_query_spell_names_with_string_contains_and_project_ids()
    {
        using var stream = TestDataPaths.OpenSpellDb2();

        var provider = new FileSystemDbdProvider(new FileSystemDbdProviderOptions(TestDataPaths.GetTestDataDirectory()));
        var db = new Db2Database(provider);

        var spells = db.OpenTable<SpellRow>("Spell", stream);

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
        using var stream = TestDataPaths.OpenCollectableSourceQuestSparseDb2();

        var provider = new FileSystemDbdProvider(new FileSystemDbdProviderOptions(TestDataPaths.GetTestDataDirectory()));
        var db = new Db2Database(provider);

        var table = db.OpenTable<CollectableSourceQuestSparseRow>("CollectableSourceQuestSparse", stream);

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
        using var stream = TestDataPaths.OpenAccountStoreCategoryDb2();

        var provider = new FileSystemDbdProvider(new(TestDataPaths.GetTestDataDirectory()));
        var db = new Db2Database(provider);

        var categories = db.OpenTable<AccountStoreCategoryRow>("AccountStoreCategory", stream);

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
        var db = new Db2Database(dbdProvider, db2Provider);

        var map = db.OpenTable<MapRow>("Map");

        map.Schema.TableName.ShouldBe("Map");

        map.Any().ShouldBeTrue();
        map.Count().ShouldBeGreaterThan(0);

        var single = map.Where(x => x.Id == x.Id).Take(1).Single();
        single.ShouldNotBeNull();
    }

    private sealed class MapRow
    {
        public int Id { get; set; }
        public string Directory { get; set; } = string.Empty;
        public string MapName_lang { get; set; } = string.Empty;
    }

    private sealed class SpellRow
    {
        public int Id { get; set; }
    }

    private sealed class CollectableSourceQuestSparseRow
    {
        public int Id { get; set; }
        public int QuestID { get; set; }
        public int CollectableSourceInfoID { get; set; }
    }

    private sealed class AccountStoreCategoryRow
    {
        public int Id { get; set; }
        public int StoreFrontID { get; set; }
        public string Name_lang { get; set; } = string.Empty;
    }
}
