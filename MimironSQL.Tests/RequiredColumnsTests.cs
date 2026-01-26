using System.Linq.Expressions;

using MimironSQL.Db2.Query;
using MimironSQL.Db2.Wdc5;
using MimironSQL.Providers;

using Shouldly;

using MimironSQL.Tests.Fixtures;

namespace MimironSQL.Tests;

public sealed class RequiredColumnsTests
{
    [Fact]
    public void Phase5_collects_required_columns_for_row_projector()
    {
        var testDataDir = TestDataPaths.GetTestDataDirectory();

        var db2Provider = new FileSystemDb2StreamProvider(new(testDataDir));
        var dbdProvider = new FileSystemDbdProvider(new(testDataDir));
        var context = new PruningTestDb2Context(dbdProvider, db2Provider);

        Expression<Func<MapWithCtor, int>> selectId = x => x.Id;
        Db2RowProjectorCompiler.TryCompile(context.Map.Schema, selectId, out _, out var reqId).ShouldBeTrue();

        reqId.Columns.Any(c => c.Kind == Db2RequiredColumnKind.Scalar && c.Field.IsId).ShouldBeTrue();

        Expression<Func<MapWithCtor, string>> selectDirectory = x => x.Directory;
        Db2RowProjectorCompiler.TryCompile(context.Map.Schema, selectDirectory, out _, out var reqDir).ShouldBeTrue();

        reqDir.Columns.Any(c => c.Kind == Db2RequiredColumnKind.String && c.Field.Name.Equals("Directory", StringComparison.OrdinalIgnoreCase)).ShouldBeTrue();
    }

    [Fact]
    public void Phase5_collects_required_columns_for_row_predicate()
    {
        var testDataDir = TestDataPaths.GetTestDataDirectory();

        var db2Provider = new FileSystemDb2StreamProvider(new(testDataDir));
        var dbdProvider = new FileSystemDbdProvider(new(testDataDir));
        var context = new PruningTestDb2Context(dbdProvider, db2Provider);

        Expression<Func<MapWithCtor, bool>> predicate = x => x.Id > 0 && x.Directory.Contains("o");
        Db2RowPredicateCompiler.TryCompile(context.Map.File, context.Map.Schema, predicate, out _, out var requirements).ShouldBeTrue();

        requirements.Columns.Any(c => c.Kind == Db2RequiredColumnKind.Scalar && c.Field.IsId).ShouldBeTrue();
        requirements.Columns.Any(c => c.Kind == Db2RequiredColumnKind.String && c.Field.Name.Equals("Directory", StringComparison.OrdinalIgnoreCase)).ShouldBeTrue();
    }

    [Fact]
    public void Phase5_attaches_per_source_requirements_to_navigation_string_predicate_plan()
    {
        var testDataDir = TestDataPaths.GetTestDataDirectory();

        var db2Provider = new FileSystemDb2StreamProvider(new(testDataDir));
        var dbdProvider = new FileSystemDbdProvider(new(testDataDir));
        var context = new TestDb2Context(dbdProvider, db2Provider);

        Expression<Func<Spell, bool>> predicate = s => s.SpellName!.Name_lang.Contains("Fire");

        Db2NavigationQueryTranslator.TryTranslateStringPredicate(context.Model, predicate, out var plan).ShouldBeTrue();

        plan.RootRequirements.Columns.Any(c => c.Kind == Db2RequiredColumnKind.JoinKey && c.Field.IsId).ShouldBeTrue();
        plan.TargetRequirements.Columns.Any(c => c.Kind == Db2RequiredColumnKind.JoinKey && c.Field.IsId).ShouldBeTrue();
        plan.TargetRequirements.Columns.Any(c => c.Kind == Db2RequiredColumnKind.String && c.Field.Name.Equals("Name_lang", StringComparison.OrdinalIgnoreCase)).ShouldBeTrue();
    }

    [Fact]
    public void Phase5_pruned_projection_decodes_fewer_fields_than_entity_materialization()
    {
        var testDataDir = TestDataPaths.GetTestDataDirectory();

        var db2Provider = new FileSystemDb2StreamProvider(new(testDataDir));
        var dbdProvider = new FileSystemDbdProvider(new(testDataDir));
        var context = new PruningTestDb2Context(dbdProvider, db2Provider);

        context.Map.Schema.TryGetField("Directory", out var directoryField).ShouldBeTrue();

        var fieldsCount = context.Map.File.Header.FieldsCount;

        Wdc5RowReadSnapshot prunedSnapshot;
        using (Wdc5RowReadTracker.Start(fieldsCount))
        {
            var pruned = context.Map
                .Where(x => x.Id > 0)
                .Select(x => x.Id)
                .Take(10)
                .ToList();

            pruned.Count.ShouldBeGreaterThan(0);
            prunedSnapshot = Wdc5RowReadTracker.Snapshot();
        }

        prunedSnapshot.StringReads[directoryField.ColumnStartIndex].ShouldBe(0);

        Wdc5RowReadSnapshot unprunedSnapshot;
        using (Wdc5RowReadTracker.Start(fieldsCount))
        {
            var unpruned = context.Map
                .Select(x => x.Id)
                .Where(id => id > 0)
                .Take(10)
                .ToList();

            unpruned.Count.ShouldBeGreaterThan(0);
            unprunedSnapshot = Wdc5RowReadTracker.Snapshot();
        }

        unprunedSnapshot.StringReads[directoryField.ColumnStartIndex].ShouldBeGreaterThan(0);
    }
}
