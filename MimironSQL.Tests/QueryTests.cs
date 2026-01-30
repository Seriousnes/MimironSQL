using MimironSQL.Db2.Query;
using MimironSQL.Db2.Schema;
using MimironSQL.Db2;
using MimironSQL.Formats;
using MimironSQL.Formats.Wdc5;
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

        var mapFile = context.GetOrOpenTableRawTyped<RowHandle>(map.TableName).File;

        var sample = mapFile.EnumerateRows()
            .Take(Math.Min(200, mapFile.RecordsCount))
            .Select(r => mapFile.ReadField<string>(r, directoryField.ColumnStartIndex))
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

        var spell = context.Spell;
        var file = context.GetOrOpenTableRawTyped<RowHandle>(spell.TableName).File;

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

        var mapFile = context.GetOrOpenTableRawTyped<RowHandle>(map.TableName).File;

        var candidate = mapFile.EnumerateRows()
            .Select(r => (Id: r.RowId, ParentId: mapFile.ReadField<int>(r, parentMapIdField.ColumnStartIndex)))
            .FirstOrDefault(x => x.ParentId > 0 && mapFile.TryGetRowById(x.ParentId, out _));

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

        var spellFile = context.GetOrOpenTableRawTyped<RowHandle>(spell.TableName).File;
        var spellNameFile = context.GetOrOpenTableRawTyped<RowHandle>(spellName.TableName).File;

        var candidateId = spellFile.EnumerateRows()
            .Select(r => r.RowId)
            .FirstOrDefault(id => id > 0 && spellNameFile.TryGetRowById(id, out _));

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

        var mapFile = context.GetOrOpenTableRawTyped<RowHandle>(map.TableName).File;

        var candidate = mapFile.EnumerateRows()
            .Select(r => (Id: r.RowId, ParentId: mapFile.ReadField<int>(r, parentMapIdField.ColumnStartIndex)))
            .FirstOrDefault(x => x.ParentId > 0 && mapFile.TryGetRowById(x.ParentId, out _));

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
            .Where(x => x.Map.MapName_lang == "Tazavesh, the Veiled Market")
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
            .Where(x => x.Map.MapName_lang.Contains("Tazavesh"))
            .ToList();

        results.Count.ShouldBeGreaterThanOrEqualTo(1);
        results.All(x => x.MapID == 2441).ShouldBeTrue();
    }

    [Fact]
    public void Phase3_supports_navigation_string_contains_in_where_with_captured_needle()
    {
        var testDataDir = TestDataPaths.GetTestDataDirectory();
        var db2Provider = new FileSystemDb2StreamProvider(new(testDataDir));
        var dbdProvider = new FileSystemDbdProvider(new(testDataDir));
        var context = new TestDb2Context(dbdProvider, db2Provider);

        var needle = "Tazavesh";

        var results = context.MapChallengeMode
            .Where(x => x.Map.MapName_lang.Contains(needle))
            .ToList();

        results.Count.ShouldBeGreaterThanOrEqualTo(1);
        results.All(x => x.MapID == 2441).ShouldBeTrue();
    }

    [Fact]
    public void Phase3_supports_navigation_string_contains_conjunction_in_where()
    {
        var testDataDir = TestDataPaths.GetTestDataDirectory();
        var db2Provider = new FileSystemDb2StreamProvider(new(testDataDir));
        var dbdProvider = new FileSystemDbdProvider(new(testDataDir));
        var context = new TestDb2Context(dbdProvider, db2Provider);

        var results = context.MapChallengeMode
            .Where(x => x.Map.MapName_lang.Contains("Tazavesh") && x.Map.MapName_lang.Contains("Veiled"))
            .ToList();

        results.Count.ShouldBe(2);
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
            .Select(x => x.Map.MapName_lang);

        names.Count().ShouldBe(2);
        names.All(n => n == "Tazavesh, the Veiled Market").ShouldBeTrue();
    }

    [Fact]
    public void Phase5_batched_navigation_projection_avoids_row_by_id_n_plus_one()
    {
        var testDataDir = TestDataPaths.GetTestDataDirectory();
        var db2Provider = new FileSystemDb2StreamProvider(new(testDataDir));
        var dbdProvider = new FileSystemDbdProvider(new(testDataDir));
        var context = new TestDb2Context(dbdProvider, db2Provider);

        Wdc5FileLookupSnapshot snapshot;
        List<string?> names;

        using (Wdc5FileLookupTracker.Start())
        {
            names = [.. context.Spell
                .Where(s => s.Id > 0)
                .Select(s => (string?)s.SpellName.Name_lang)
                .Take(50)];

            snapshot = Wdc5FileLookupTracker.Snapshot();
        }

        names.Count.ShouldBeGreaterThan(0);
        names.Any(s => !string.IsNullOrWhiteSpace(s)).ShouldBeTrue();

        snapshot.TotalTryGetRowByIdCalls.ShouldBe(0);
    }

    [Fact]
    public void Phase4_include_is_batched_for_schema_fk_navigation_and_avoids_row_by_id_n_plus_one()
    {
        var testDataDir = TestDataPaths.GetTestDataDirectory();
        var db2Provider = new FileSystemDb2StreamProvider(new(testDataDir));
        var dbdProvider = new FileSystemDbdProvider(new(testDataDir));
        var context = new TestDb2Context(dbdProvider, db2Provider);

        var map = context.Map;
        var parentMapIdField = map.Schema.Fields.First(f => f.Name.Equals("ParentMapID", StringComparison.OrdinalIgnoreCase));

        var mapFile = context.GetOrOpenTableRawTyped<RowHandle>(map.TableName).File;

        var allIds = mapFile.EnumerateRows().Select(r => r.RowId).ToHashSet();

        var candidate = mapFile.EnumerateRows()
            .Select(r => (Id: r.RowId, ParentId: mapFile.ReadField<int>(r, parentMapIdField.ColumnStartIndex)))
            .FirstOrDefault(x => x.ParentId > 0 && allIds.Contains(x.ParentId));

        candidate.ParentId.ShouldBeGreaterThan(0);

        Wdc5FileLookupSnapshot snapshot;
        Map entity;

        using (Wdc5FileLookupTracker.Start())
        {
            entity = map
                .Where(x => x.Id == candidate.Id)
                .Include(x => x.ParentMap)
                .Single();

            snapshot = Wdc5FileLookupTracker.Snapshot();
        }

        entity.ParentMapID.ShouldBe(candidate.ParentId);
        entity.ParentMap.ShouldNotBeNull();
        entity.ParentMap!.Id.ShouldBe(candidate.ParentId);

        snapshot.TotalTryGetRowByIdCalls.ShouldBe(0);
    }

    [Fact]
    public void Phase4_include_is_batched_for_shared_primary_key_navigation_and_avoids_row_by_id_n_plus_one()
    {
        var testDataDir = TestDataPaths.GetTestDataDirectory();
        var db2Provider = new FileSystemDb2StreamProvider(new(testDataDir));
        var dbdProvider = new FileSystemDbdProvider(new(testDataDir));
        var context = new TestDb2Context(dbdProvider, db2Provider);

        var spell = context.Spell;
        var spellName = context.SpellName;

        var spellFile = context.GetOrOpenTableRawTyped<RowHandle>(spell.TableName).File;
        var spellNameFile = context.GetOrOpenTableRawTyped<RowHandle>(spellName.TableName).File;

        var spellNameIds = spellNameFile.EnumerateRows().Select(r => r.RowId).ToHashSet();
        var candidateId = spellFile.EnumerateRows().Select(r => r.RowId).FirstOrDefault(id => id > 0 && spellNameIds.Contains(id));

        candidateId.ShouldBeGreaterThan(0);

        Wdc5FileLookupSnapshot snapshot;
        Spell entity;

        using (Wdc5FileLookupTracker.Start())
        {
            entity = spell
                .Where(s => s.Id == candidateId)
                .Include(s => s.SpellName)
                .Single();

            snapshot = Wdc5FileLookupTracker.Snapshot();
        }

        entity.SpellName.ShouldNotBeNull();
        entity.SpellName!.Id.ShouldBe(candidateId);

        snapshot.TotalTryGetRowByIdCalls.ShouldBe(0);
    }

    [Fact]
    public void Phase3_supports_navigation_scalar_equal_predicate()
    {
        var testDataDir = TestDataPaths.GetTestDataDirectory();
        var db2Provider = new FileSystemDb2StreamProvider(new(testDataDir));
        var dbdProvider = new FileSystemDbdProvider(new(testDataDir));
        var context = new TestDb2Context(dbdProvider, db2Provider);

        var targetMapId = 2441;

        // First verify the data exists
        var mapExists = context.Map.Where(m => m.Id == targetMapId).Any();
        mapExists.ShouldBeTrue($"Map with Id {targetMapId} should exist");

        var mcmWithMapId = context.MapChallengeMode.Where(x => x.MapID == targetMapId).ToList();
        mcmWithMapId.Count.ShouldBeGreaterThan(0, "Should have MapChallengeMode records with MapID 2441");

        // Now test the navigation predicate
        var results = context.MapChallengeMode
            .Where(x => x.Map.Id == targetMapId)
            .ToList();

        results.Count.ShouldBeGreaterThanOrEqualTo(1);
        results.All(x => x.MapID == targetMapId).ShouldBeTrue();
    }

    [Fact]
    public void Query_includes_array_collection_values()
    {
        var testDataDir = TestDataPaths.GetTestDataDirectory();
        var db2Provider = new FileSystemDb2StreamProvider(new(testDataDir));
        var dbdProvider = new FileSystemDbdProvider(new(testDataDir));
        var context = new TestDb2Context(dbdProvider, db2Provider);

        var mcm = context.MapChallengeMode.Find(56);
        mcm.ShouldNotBeNull();
        mcm.FirstRewardQuestID.Count.ShouldBe(6);
        mcm.FirstRewardQuestID.Take(4).ShouldAllBe(x => x > 0);
    }

    [Fact]
    public void Include_populates_collection_navigation_from_id_array_preserving_slots()
    {
        var testDataDir = TestDataPaths.GetTestDataDirectory();
        var db2Provider = new FileSystemDb2StreamProvider(new(testDataDir));
        var dbdProvider = new FileSystemDbdProvider(new(testDataDir));
        var context = new TestDb2Context(dbdProvider, db2Provider);

        var mcm = context.MapChallengeMode
            .Where(x => x.Id == 56)
            .Include(x => x.FirstRewardQuest)
            .Single();

        mcm.FirstRewardQuestID.Count.ShouldBe(6);
        mcm.FirstRewardQuest.Count.ShouldBe(6);

        var ids = mcm.FirstRewardQuestID.ToArray();
        var quests = mcm.FirstRewardQuest.ToArray();

        var questFile = context.GetOrOpenTableRawTyped<RowHandle>(nameof(QuestV2)).File;
        var questIds = questFile.EnumerateRows().Select(r => r.RowId).ToHashSet();

        for (var i = 0; i < ids.Length; i++)
        {
            if (ids[i] == 0)
            {
                quests[i].ShouldBeNull();
                continue;
            }

            if (questIds.Contains(ids[i]))
            {
                quests[i].ShouldNotBeNull();
                quests[i]!.Id.ShouldBe(ids[i]);
            }
            else
            {
                quests[i].ShouldBeNull();
            }
        }
    }

    [Fact]
    public void Include_populates_inverse_fk_collection_navigation_as_empty_or_populated()
    {
        var testDataDir = TestDataPaths.GetTestDataDirectory();
        var db2Provider = new FileSystemDb2StreamProvider(new(testDataDir));
        var dbdProvider = new FileSystemDbdProvider(new(testDataDir));
        var context = new TestDb2Context(dbdProvider, db2Provider);

        var mapChallengeModeFile = context.GetOrOpenTableRawTyped<RowHandle>(nameof(MapChallengeMode)).File;
        var mapChallengeModeSchema = context.GetOrOpenTableRawTyped<RowHandle>(nameof(MapChallengeMode)).Schema;

        mapChallengeModeSchema.TryGetField("MapID", out var mapIdField).ShouldBeTrue();
        var mapIdsWithChallengeModes = mapChallengeModeFile.EnumerateRows()
            .Select(r => (int)mapChallengeModeFile.ReadField<ushort>(r, mapIdField.ColumnStartIndex))
            .Where(id => id != 0)
            .ToHashSet();

        var mapFile = context.GetOrOpenTableRawTyped<RowHandle>(nameof(Map)).File;
        var mapIds = mapFile.EnumerateRows().Select(r => r.RowId).Where(id => id != 0).ToList();

        var populatedMapId = mapIds.First(id => mapIdsWithChallengeModes.Contains(id));
        var emptyMapId = mapIds.First(id => !mapIdsWithChallengeModes.Contains(id));

        var populated = context.Map
            .Where(m => m.Id == populatedMapId)
            .Include(m => m.MapChallengeModes)
            .Single();

        populated.MapChallengeModes.Count.ShouldBeGreaterThan(0);
        populated.MapChallengeModes.All(mcm => mcm.MapID == populatedMapId).ShouldBeTrue();

        var empty = context.Map
            .Where(m => m.Id == emptyMapId)
            .Include(m => m.MapChallengeModes)
            .Single();

        empty.MapChallengeModes.Count.ShouldBe(0);
    }

    [Fact]
    public void Where_supports_collection_any_semi_join()
    {
        var testDataDir = TestDataPaths.GetTestDataDirectory();
        var db2Provider = new FileSystemDb2StreamProvider(new(testDataDir));
        var dbdProvider = new FileSystemDbdProvider(new(testDataDir));
        var context = new TestDb2Context(dbdProvider, db2Provider);

        var mapChallengeModeFile = context.GetOrOpenTableRawTyped<RowHandle>(nameof(MapChallengeMode)).File;
        var mapChallengeModeSchema = context.GetOrOpenTableRawTyped<RowHandle>(nameof(MapChallengeMode)).Schema;
        mapChallengeModeSchema.TryGetField("MapID", out var mapIdField).ShouldBeTrue();
        var expected = mapChallengeModeFile.EnumerateRows()
            .Select(r => (int)mapChallengeModeFile.ReadField<ushort>(r, mapIdField.ColumnStartIndex))
            .Where(id => id != 0)
            .ToHashSet();

        var results = context.Map
            .Where(m => m.MapChallengeModes.Any())
            .Take(50)
            .ToList();

        results.Count.ShouldBeGreaterThan(0);
        results.All(m => expected.Contains(m.Id)).ShouldBeTrue();
    }

    [Fact]
    public void Where_supports_collection_any_with_scalar_predicate()
    {
        var testDataDir = TestDataPaths.GetTestDataDirectory();
        var db2Provider = new FileSystemDb2StreamProvider(new(testDataDir));
        var dbdProvider = new FileSystemDbdProvider(new(testDataDir));
        var context = new TestDb2Context(dbdProvider, db2Provider);

        var mapChallengeModeFile = context.GetOrOpenTableRawTyped<RowHandle>(nameof(MapChallengeMode)).File;
        var mapChallengeModeSchema = context.GetOrOpenTableRawTyped<RowHandle>(nameof(MapChallengeMode)).Schema;
        mapChallengeModeSchema.TryGetField("MapID", out var mapIdField).ShouldBeTrue();

        var first = mapChallengeModeFile.EnumerateRows()
            .Select(r => (Id: r.RowId, MapId: (int)mapChallengeModeFile.ReadField<ushort>(r, mapIdField.ColumnStartIndex)))
            .First(x => x.Id != 0 && x.MapId != 0);

        var result = context.Map
            .Where(m => m.MapChallengeModes.Any(mc => mc.Id == first.Id))
            .Single();

        result.Id.ShouldBe(first.MapId);
    }

    [Fact]
    public void Phase3_supports_navigation_scalar_not_equal_predicate()
    {
        var testDataDir = TestDataPaths.GetTestDataDirectory();
        var db2Provider = new FileSystemDb2StreamProvider(new(testDataDir));
        var dbdProvider = new FileSystemDbdProvider(new(testDataDir));
        var context = new TestDb2Context(dbdProvider, db2Provider);

        var excludeMapId = 2441;
        var results = context.MapChallengeMode
            .Where(x => x.Map.Id != excludeMapId)
            .Take(10)
            .ToList();

        results.Count.ShouldBeGreaterThan(0);
        results.All(x => x.MapID != excludeMapId).ShouldBeTrue();
    }

    [Fact]
    public void Phase3_supports_navigation_scalar_greater_than_predicate()
    {
        var testDataDir = TestDataPaths.GetTestDataDirectory();
        var db2Provider = new FileSystemDb2StreamProvider(new(testDataDir));
        var dbdProvider = new FileSystemDbdProvider(new(testDataDir));
        var context = new TestDb2Context(dbdProvider, db2Provider);

        var threshold = 2000;
        var results = context.MapChallengeMode
            .Where(x => x.Map.Id > threshold)
            .Take(10)
            .ToList();

        results.Count.ShouldBeGreaterThan(0);
        results.All(x => x.MapID > threshold).ShouldBeTrue();
    }

    [Fact]
    public void Phase3_supports_navigation_scalar_less_than_or_equal_predicate()
    {
        var testDataDir = TestDataPaths.GetTestDataDirectory();
        var db2Provider = new FileSystemDb2StreamProvider(new(testDataDir));
        var dbdProvider = new FileSystemDbdProvider(new(testDataDir));
        var context = new TestDb2Context(dbdProvider, db2Provider);

        var threshold = 3000;
        var results = context.MapChallengeMode
            .Where(x => x.Map.Id <= threshold)
            .Take(10)
            .ToList();

        results.Count.ShouldBeGreaterThan(0);
        results.All(x => x.MapID <= threshold).ShouldBeTrue();
    }

    [Fact]
    public void Phase3_supports_navigation_null_check_not_null()
    {
        var testDataDir = TestDataPaths.GetTestDataDirectory();
        var db2Provider = new FileSystemDb2StreamProvider(new(testDataDir));
        var dbdProvider = new FileSystemDbdProvider(new(testDataDir));
        var context = new TestDb2Context(dbdProvider, db2Provider);

        var results = context.MapChallengeMode
            .Where(x => x.Map != null)
            .Take(10)
            .ToList();

        results.Count.ShouldBeGreaterThan(0);
        results.All(x => x.MapID > 0).ShouldBeTrue();
    }

    [Fact]
    public void Phase3_supports_navigation_null_check_with_scalar_predicate()
    {
        var testDataDir = TestDataPaths.GetTestDataDirectory();
        var db2Provider = new FileSystemDb2StreamProvider(new(testDataDir));
        var dbdProvider = new FileSystemDbdProvider(new(testDataDir));
        var context = new TestDb2Context(dbdProvider, db2Provider);

        var targetMapId = 2441;

        // Test the combined predicate directly
        var results = context.MapChallengeMode
            .Where(x => x.Map != null && x.Map.Id == targetMapId)
            .ToList();

        results.Count.ShouldBeGreaterThanOrEqualTo(1);
        results.All(x => x.MapID == targetMapId).ShouldBeTrue();
    }

    [Fact]
    public void Navigation_scalar_predicate_does_not_throw_when_navigation_is_missing_and_behaves_like_inner_join()
    {
        var testDataDir = TestDataPaths.GetTestDataDirectory();
        var db2Provider = new FileSystemDb2StreamProvider(new(testDataDir));
        var dbdProvider = new FileSystemDbdProvider(new(testDataDir));
        var context = new TestDb2Context(dbdProvider, db2Provider);

        var map = context.Map;
        var mapFile = context.GetOrOpenTableRawTyped<RowHandle>(map.TableName).File;
        var allMapIds = mapFile.EnumerateRows().Select(r => r.RowId).ToHashSet();

        // Ensure the fixture has at least one missing parent navigation (ParentMapID == 0).
        map.Where(m => m.ParentMapID == 0).Take(1).Any().ShouldBeTrue("Fixture should contain at least one Map row with ParentMapID == 0");

        List<Map> results = [];
        Should.NotThrow(() =>
            results = [.. map
                .Where(m => m.ParentMap.Id > 0 && m.Directory.Contains("o"))
                .Take(50)]);

        results.All(r => r.ParentMapID != 0 && allMapIds.Contains(r.ParentMapID)).ShouldBeTrue();
    }

    [Fact]
    public void Navigation_null_check_is_null_returns_missing_navigations()
    {
        var testDataDir = TestDataPaths.GetTestDataDirectory();
        var db2Provider = new FileSystemDb2StreamProvider(new(testDataDir));
        var dbdProvider = new FileSystemDbdProvider(new(testDataDir));
        var context = new TestDb2Context(dbdProvider, db2Provider);

        var map = context.Map;
        var mapFile = context.GetOrOpenTableRawTyped<RowHandle>(map.TableName).File;
        var allMapIds = mapFile.EnumerateRows().Select(r => r.RowId).ToHashSet();

        var results = map
            .Where(m => m.ParentMap == null)
            .Take(50)
            .ToList();

        results.Count.ShouldBeGreaterThan(0);
        results.All(r => r.ParentMapID == 0 || !allMapIds.Contains(r.ParentMapID)).ShouldBeTrue();
    }

    [Fact]
    public void Navigation_null_check_is_not_null_filters_to_existing_navigations()
    {
        var testDataDir = TestDataPaths.GetTestDataDirectory();
        var db2Provider = new FileSystemDb2StreamProvider(new(testDataDir));
        var dbdProvider = new FileSystemDbdProvider(new(testDataDir));
        var context = new TestDb2Context(dbdProvider, db2Provider);

        var map = context.Map;
        var mapFile = context.GetOrOpenTableRawTyped<RowHandle>(map.TableName).File;
        var allMapIds = mapFile.EnumerateRows().Select(r => r.RowId).ToHashSet();

        var results = map
            .Where(m => m.ParentMap != null)
            .Take(50)
            .ToList();

        results.Count.ShouldBeGreaterThan(0);
        results.All(r => r.ParentMapID != 0 && allMapIds.Contains(r.ParentMapID)).ShouldBeTrue();
    }

    [Fact]
    public void Phase3_supports_scalar_range_predicate_with_intersection()
    {
        var testDataDir = TestDataPaths.GetTestDataDirectory();
        var db2Provider = new FileSystemDb2StreamProvider(new(testDataDir));
        var dbdProvider = new FileSystemDbdProvider(new(testDataDir));
        var context = new TestDb2Context(dbdProvider, db2Provider);

        var minId = 2000;
        var maxId = 3000;

        var results = context.MapChallengeMode
            .Where(x => x.Map!.Id > minId && x.Map!.Id <= maxId)
            .ToList();

        results.Count.ShouldBeGreaterThan(0);
        results.All(x => x.MapID > minId && x.MapID <= maxId).ShouldBeTrue();
    }

    [Fact]
    public void Phase3_supports_string_and_scalar_predicate_intersection_on_same_navigation()
    {
        var testDataDir = TestDataPaths.GetTestDataDirectory();
        var db2Provider = new FileSystemDb2StreamProvider(new(testDataDir));
        var dbdProvider = new FileSystemDbdProvider(new(testDataDir));
        var context = new TestDb2Context(dbdProvider, db2Provider);

        var results = context.MapChallengeMode
            .Where(x => x.Map!.MapName_lang.Contains("Tazavesh") && x.Map!.Id == 2441)
            .ToList();

        results.Count.ShouldBeGreaterThanOrEqualTo(1);
        results.All(x => x.MapID == 2441).ShouldBeTrue();
    }

}
