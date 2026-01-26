using MimironSQL.Db2.Query;
using MimironSQL.Db2.Schema;
using MimironSQL.Db2.Wdc5;
using MimironSQL.Db2;
using MimironSQL.Providers;

using Shouldly;
using MimironSQL.Tests.Fixtures;

namespace MimironSQL.Tests;

public class QueryTests
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
            .Take(25);

        results.Count().ShouldBeGreaterThan(0);
        results.Any(x => x.Id > 0).ShouldBeTrue();
    }

    [Fact]
    public void Can_query_dense_table_with_startswith_and_endswith()
    {
        var testDataDir = TestDataPaths.GetTestDataDirectory();
        var db2Provider = new FileSystemDb2StreamProvider(new(testDataDir));
        var dbdProvider = new FileSystemDbdProvider(new(testDataDir));
        var context = new TestDb2Context(dbdProvider, db2Provider);

        var map = context.Map;
        map.Schema.TryGetField("Directory", out var directoryField).ShouldBeTrue();

        var sample = map.File.EnumerateRows()
            .Take(Math.Min(200, map.File.Header.RecordsCount))
            .Select(r => r.TryGetString(directoryField.ColumnStartIndex, out var s) ? s : string.Empty)
            .FirstOrDefault(s => !string.IsNullOrWhiteSpace(s));

        sample.ShouldNotBeNull();
        sample.Length.ShouldBeGreaterThanOrEqualTo(2);

        var prefix = sample[..2];
        var suffix = sample[^2..];

        context.Map.Where(x => x.Directory.StartsWith(prefix)).Take(10).Count().ShouldBeGreaterThan(0);
        context.Map.Where(x => x.Directory.EndsWith(suffix)).Take(10).Count().ShouldBeGreaterThan(0);
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
            .Take(20);

        ids.Count().ShouldBeGreaterThanOrEqualTo(1);
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
            .Take(50);

        results.Count().ShouldBeGreaterThan(0);
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

    [Fact]
    public void Phase1_requires_explicit_key_when_entity_has_no_id_member()
    {
        var testDataDir = TestDataPaths.GetTestDataDirectory();
        var db2Provider = new FileSystemDb2StreamProvider(new(testDataDir));
        var dbdProvider = new FileSystemDbdProvider(new(testDataDir));

        var ex = Should.Throw<NotSupportedException>(() => _ = new NoKeyTestDb2Context(dbdProvider, db2Provider));
        ex.Message.ShouldContain("has no key member");
    }

    [Fact]
    public void Phase35_prunes_scalar_projection_without_entity_materialization()
    {
        MapWithCtor.InstancesCreated = 0;

        var testDataDir = TestDataPaths.GetTestDataDirectory();

        var db2Provider = new FileSystemDb2StreamProvider(new(testDataDir));
        var dbdProvider = new FileSystemDbdProvider(new(testDataDir));
        var context = new PruningTestDb2Context(dbdProvider, db2Provider);

        var ids = context.Map
            .Where(x => x.Id > 0)
            .Select(x => x.Id)
            .Take(10);

        ids.Count().ShouldBeGreaterThan(0);
        MapWithCtor.InstancesCreated.ShouldBe(0);
    }

    [Fact]
    public void Phase35_prunes_anonymous_and_dto_projections_without_entity_materialization()
    {
        MapWithCtor.InstancesCreated = 0;

        var testDataDir = TestDataPaths.GetTestDataDirectory();

        var db2Provider = new FileSystemDb2StreamProvider(new(testDataDir));
        var dbdProvider = new FileSystemDbdProvider(new(testDataDir));
        var context = new PruningTestDb2Context(dbdProvider, db2Provider);

        var anon = context.Map
            .Where(x => x.Id > 0)
            .Select(x => new { x.Id, x.Directory })
            .Take(5);

        anon.Count().ShouldBeGreaterThan(0);
        anon.All(x => x.Id > 0).ShouldBeTrue();

        var dtos = context.Map
            .Where(x => x.Id > 0)
            .Select(x => new MapDto(x.Id, x.Directory))
            .Take(5);

        dtos.Count().ShouldBeGreaterThan(0);
        dtos.All(x => x.Id > 0).ShouldBeTrue();
        MapWithCtor.InstancesCreated.ShouldBe(0);
    }

    [Fact]
    public void Phase35_does_not_prune_when_where_is_after_select_shape_2()
    {
        MapWithCtor.InstancesCreated = 0;

        var testDataDir = TestDataPaths.GetTestDataDirectory();

        var db2Provider = new FileSystemDb2StreamProvider(new(testDataDir));
        var dbdProvider = new FileSystemDbdProvider(new(testDataDir));
        var context = new PruningTestDb2Context(dbdProvider, db2Provider);

        var ids = context.Map
            .Select(x => x.Id)
            .Where(id => id > 0)
            .Take(5);

        ids.Count().ShouldBeGreaterThan(0);
        MapWithCtor.InstancesCreated.ShouldBeGreaterThan(0);
    }

    [Fact]
    public void Find_returns_entity_when_present()
    {
        var testDataDir = TestDataPaths.GetTestDataDirectory();

        var db2Provider = new FileSystemDb2StreamProvider(new(testDataDir));
        var dbdProvider = new FileSystemDbdProvider(new(testDataDir));
        var context = new TestDb2Context(dbdProvider, db2Provider);

        var id = context.Spell.Select(x => x.Id).Take(1).First();
        var found = context.Spell.Find(id);

        found.ShouldNotBeNull();
        found!.Id.ShouldBe(id);
    }

    [Fact]
    public void Find_returns_null_when_missing()
    {
        var testDataDir = TestDataPaths.GetTestDataDirectory();

        var db2Provider = new FileSystemDb2StreamProvider(new(testDataDir));
        var dbdProvider = new FileSystemDbdProvider(new(testDataDir));
        var context = new TestDb2Context(dbdProvider, db2Provider);

        var file = context.Spell.File;

        var missing = int.MaxValue;
        for (var i = 0; i < 1_000 && file.TryGetRowById(missing, out _); i++)
            missing--;

        file.TryGetRowById(missing, out _).ShouldBeFalse();

        var found = context.Spell.Find(missing);
        found.ShouldBeNull();
    }

    [Fact]
    public void Find_supports_byte_ids()
    {
        var testDataDir = TestDataPaths.GetTestDataDirectory();

        var db2Provider = new FileSystemDb2StreamProvider(new(testDataDir));
        var dbdProvider = new FileSystemDbdProvider(new(testDataDir));
        var context = new TestDb2Context(dbdProvider, db2Provider);

        var id = context.GarrType.Select(x => x.Id).Take(1).First();
        var found = context.GarrType.Find(id);

        found.ShouldNotBeNull();
        found!.Id.ShouldBe(id);
    }

    [Fact]
    public void Phase4_include_populates_reference_navigation_when_row_exists()
    {
        var testDataDir = TestDataPaths.GetTestDataDirectory();
        var db2Provider = new FileSystemDb2StreamProvider(new(testDataDir));
        var dbdProvider = new FileSystemDbdProvider(new(testDataDir));
        var context = new TestDb2Context(dbdProvider, db2Provider);

        var map = context.Map;
        var parentMapIdField = map.Schema.Fields.First(f => f.Name.Equals("ParentMapID", StringComparison.OrdinalIgnoreCase));

        var candidate = map.File.EnumerateRows()
            .Select(r => (Id: r.Id, ParentId: Convert.ToInt32(r.GetScalar<long>(parentMapIdField.ColumnStartIndex))))
            .FirstOrDefault(x => x.ParentId > 0 && map.File.TryGetRowById(x.ParentId, out _));

        candidate.ParentId.ShouldBeGreaterThan(0);

        var entity = map
            .Where(x => x.Id == candidate.Id)
            .Include(x => x.ParentMap)
            .Single();

        entity.ParentMapID.ShouldBe(candidate.ParentId);
        entity.ParentMap.ShouldNotBeNull();
        entity.ParentMap!.Id.ShouldBe(candidate.ParentId);
    }

    [Fact]
    public void Phase4_include_populates_shared_primary_key_navigation_when_row_exists()
    {
        var testDataDir = TestDataPaths.GetTestDataDirectory();
        var db2Provider = new FileSystemDb2StreamProvider(new(testDataDir));
        var dbdProvider = new FileSystemDbdProvider(new(testDataDir));
        var context = new TestDb2Context(dbdProvider, db2Provider);

        var spell = context.Spell;
        var spellName = context.SpellName;

        var candidateId = spell.File.EnumerateRows()
            .Select(r => r.Id)
            .FirstOrDefault(id => id > 0 && spellName.File.TryGetRowById(id, out _));

        candidateId.ShouldBeGreaterThan(0);

        var entity = spell
            .Where(s => s.Id == candidateId)
            .Include(s => s.SpellName)
            .Single();

        entity.SpellName.ShouldNotBeNull();
        entity.SpellName!.Id.ShouldBe(candidateId);
    }

    [Fact]
    public void Phase4_include_throws_on_schema_fk_and_model_navigation_conflict_without_override()
    {
        var testDataDir = TestDataPaths.GetTestDataDirectory();
        var db2Provider = new FileSystemDb2StreamProvider(new(testDataDir));
        var dbdProvider = new FileSystemDbdProvider(new(testDataDir));

        var ex = Should.Throw<NotSupportedException>(() =>
            _ = new SchemaFkConflictTestDb2Context(dbdProvider, db2Provider));

        ex.Message.ShouldContain("conflicts with schema FK");
    }

    [Fact]
    public void Phase4_include_allows_model_navigation_to_override_schema_fk_when_configured()
    {
        var testDataDir = TestDataPaths.GetTestDataDirectory();
        var db2Provider = new FileSystemDb2StreamProvider(new(testDataDir));
        var dbdProvider = new FileSystemDbdProvider(new(testDataDir));
        var context = new SchemaFkOverrideTestDb2Context(dbdProvider, db2Provider);

        var map = context.Map;
        var parentMapIdField = map.Schema.Fields.First(f => f.Name.Equals("ParentMapID", StringComparison.OrdinalIgnoreCase));

        var candidate = map.File.EnumerateRows()
            .Select(r => (Id: r.Id, ParentId: Convert.ToInt32(r.GetScalar<long>(parentMapIdField.ColumnStartIndex))))
            .FirstOrDefault(x => x.ParentId > 0 && map.File.TryGetRowById(x.ParentId, out _));

        candidate.ParentId.ShouldBeGreaterThan(0);

        var entity = map
            .Where(x => x.Id == candidate.Id)
            .Include(x => x.ParentMap)
            .Single();

        entity.ParentMapID.ShouldBe(candidate.ParentId);
        entity.ParentMap.ShouldNotBeNull();
        entity.ParentMap!.Id.ShouldBe(candidate.ParentId);
    }

    [Theory]
    [InlineData(107, "Passive", "Gives a chance to block enemy melee and ranged attacks.", null)]
    [InlineData(35200, "Shapeshift", "Shapeshifts into a roc for $d., increasing armor and hit points, as well as allowing the use of various bear abilities.", "Shapeshifted into roc.\r\nArmor and hit points increased.")]
    public void Spell_materialization_matches_raw_row_for_layout_E3D134FB(int id, string? expectedNameSubtext, string? expectedDescription, string? expectedAuraDescription)
    {
        var testDataDir = TestDataPaths.GetTestDataDirectory();

        var db2Provider = new FileSystemDb2StreamProvider(new(testDataDir));
        var dbdProvider = new FileSystemDbdProvider(new(testDataDir));
        var context = new TestDb2Context(dbdProvider, db2Provider);

        var expected = new Spell
        {
            Id = id,
            NameSubtext_lang = expectedNameSubtext ?? string.Empty,
            Description_lang = expectedDescription ?? string.Empty,
            AuraDescription_lang = expectedAuraDescription ?? string.Empty
        };

        var byFind = context.Spell.Find(id);
        byFind.ShouldNotBeNull();
        byFind.ShouldBeEquivalentTo(expected);

        var byQuery = context.Spell.Where(s => s.Id == id).SingleOrDefault();
        byQuery.ShouldNotBeNull();
        byQuery.ShouldBeEquivalentTo(expected);
    }

    [Fact]    
    public void Test_Tazavesh_map_query()
    {
        var testDataDir = TestDataPaths.GetTestDataDirectory();
        var db2Provider = new FileSystemDb2StreamProvider(new(testDataDir));
        var dbdProvider = new FileSystemDbdProvider(new(testDataDir));
        var context = new TestDb2Context(dbdProvider, db2Provider);

        // Map ID = 2441 is used for two MapChallengeMode entries (Tazavesh, So'leah's Gambit (ID=392), and Tazavesh, Streets of Wonder (ID=391))        

        var results = context.MapChallengeMode
            .Where(x => x.Map!.MapName_lang == "Tazavesh, the Veiled Market")
            .ToList();

        results.Count.ShouldBe(2);
        results.All(x => x.MapID == 2441).ShouldBeTrue();
    }

    [Fact]
    public void Phase3_supports_navigation_string_contains_in_where()
    {
        var testDataDir = TestDataPaths.GetTestDataDirectory();
        var db2Provider = new FileSystemDb2StreamProvider(new(testDataDir));
        var dbdProvider = new FileSystemDbdProvider(new(testDataDir));
        var context = new TestDb2Context(dbdProvider, db2Provider);

        var results = context.MapChallengeMode
            .Where(x => x.Map!.MapName_lang.Contains("Tazavesh"))
            .ToList();

        results.Count.ShouldBeGreaterThanOrEqualTo(1);
        results.All(x => x.MapID == 2441).ShouldBeTrue();
    }

    [Fact]
    public void Phase3_supports_navigation_access_in_select_without_explicit_include()
    {
        var testDataDir = TestDataPaths.GetTestDataDirectory();
        var db2Provider = new FileSystemDb2StreamProvider(new(testDataDir));
        var dbdProvider = new FileSystemDbdProvider(new(testDataDir));
        var context = new TestDb2Context(dbdProvider, db2Provider);

        var names = context.MapChallengeMode
            .Where(x => x.MapID == 2441)
            .Select(x => x.Map!.MapName_lang);

        names.Count().ShouldBe(2);
        names.All(n => n == "Tazavesh, the Veiled Market").ShouldBeTrue();
    }

}
