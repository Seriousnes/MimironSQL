using System.Linq.Expressions;

using MimironSQL.Db2;
using MimironSQL.Db2.Model;
using MimironSQL.Db2.Query;
using MimironSQL.Db2.Schema;
using MimironSQL.Formats;

using Shouldly;

namespace MimironSQL.EntityFrameworkCore.Tests;

public sealed class Db2NavigationQueryCompilerTests
{
    [Fact]
    public void SemiJoin_scalar_predicate_on_reference_navigation_filters_root_rows()
    {
        var model = BuildParentChildModel();

        var parents = InMemoryDb2File.Create(
            tableName: nameof(Parent),
            flags: Db2Flags.None,
            denseStringTableBytes: ReadOnlyMemory<byte>.Empty,
            rows:
            [
                new InMemoryDb2File.Row(1, [1, 1, "abc"]),
                new InMemoryDb2File.Row(2, [2, 5, "zzz"]),
            ]);

        var children = InMemoryDb2File.Create(
            tableName: nameof(Child),
            flags: Db2Flags.None,
            denseStringTableBytes: ReadOnlyMemory<byte>.Empty,
            rows:
            [
                new InMemoryDb2File.Row(100, [100, 1, "c1"]),
                new InMemoryDb2File.Row(101, [101, 2, "c2"]),
            ]);

        (IDb2File<RowHandle> File, Db2TableSchema Schema) TableResolver(string tableName)
            => tableName switch
            {
                nameof(Parent) => (parents, SchemaResolver(tableName)),
                nameof(Child) => (children, SchemaResolver(tableName)),
                _ => throw new InvalidOperationException($"Unknown table: {tableName}"),
            };

        Expression<Func<Child, bool>> predicate = c => c.Parent!.Level > 3;

        Db2NavigationQueryCompiler.TryCompileSemiJoinPredicate(model, children, TableResolver, predicate, out var rowPredicate)
            .ShouldBeTrue();

        var matching = children
            .EnumerateRows()
            .Where(rowPredicate)
            .Select(r => r.RowId)
            .ToArray();

        matching.ShouldBe([101]);
    }

    [Fact]
    public void SemiJoin_null_check_and_scalar_predicate_intersects_target_ids()
    {
        var model = BuildParentChildModel();

        var parents = InMemoryDb2File.Create(
            tableName: nameof(Parent),
            flags: Db2Flags.None,
            denseStringTableBytes: ReadOnlyMemory<byte>.Empty,
            rows:
            [
                new InMemoryDb2File.Row(1, [1, 1, "abc"]),
                new InMemoryDb2File.Row(2, [2, 5, "zzz"]),
            ]);

        var children = InMemoryDb2File.Create(
            tableName: nameof(Child),
            flags: Db2Flags.None,
            denseStringTableBytes: ReadOnlyMemory<byte>.Empty,
            rows:
            [
                new InMemoryDb2File.Row(100, [100, 0, "c0"]),
                new InMemoryDb2File.Row(101, [101, 2, "c2"]),
                new InMemoryDb2File.Row(102, [102, 12345, "c-missing"]),
            ]);

        (IDb2File<RowHandle> File, Db2TableSchema Schema) TableResolver(string tableName)
            => tableName switch
            {
                nameof(Parent) => (parents, SchemaResolver(tableName)),
                nameof(Child) => (children, SchemaResolver(tableName)),
                _ => throw new InvalidOperationException($"Unknown table: {tableName}"),
            };

        Expression<Func<Child, bool>> predicate = c => c.Parent != null && c.Parent.Level > 3;

        Db2NavigationQueryCompiler.TryCompileSemiJoinPredicate(model, children, TableResolver, predicate, out var rowPredicate)
            .ShouldBeTrue();

        var matching = children
            .EnumerateRows()
            .Where(rowPredicate)
            .Select(r => r.RowId)
            .ToArray();

        matching.ShouldBe([101]);
    }

    [Fact]
    public void SemiJoin_scalar_predicate_and_null_check_intersects_target_ids()
    {
        var model = BuildParentChildModel();

        var parents = InMemoryDb2File.Create(
            tableName: nameof(Parent),
            flags: Db2Flags.None,
            denseStringTableBytes: ReadOnlyMemory<byte>.Empty,
            rows:
            [
                new InMemoryDb2File.Row(1, [1, 1, "abc"]),
                new InMemoryDb2File.Row(2, [2, 5, "zzz"]),
            ]);

        var children = InMemoryDb2File.Create(
            tableName: nameof(Child),
            flags: Db2Flags.None,
            denseStringTableBytes: ReadOnlyMemory<byte>.Empty,
            rows:
            [
                new InMemoryDb2File.Row(100, [100, 0, "c0"]),
                new InMemoryDb2File.Row(101, [101, 2, "c2"]),
                new InMemoryDb2File.Row(102, [102, 12345, "c-missing"]),
            ]);

        (IDb2File<RowHandle> File, Db2TableSchema Schema) TableResolver(string tableName)
            => tableName switch
            {
                nameof(Parent) => (parents, SchemaResolver(tableName)),
                nameof(Child) => (children, SchemaResolver(tableName)),
                _ => throw new InvalidOperationException($"Unknown table: {tableName}"),
            };

        Expression<Func<Child, bool>> predicate = c => c.Parent!.Level > 3 && c.Parent != null;

        Db2NavigationQueryCompiler.TryCompileSemiJoinPredicate(model, children, TableResolver, predicate, out var rowPredicate)
            .ShouldBeTrue();

        children.EnumerateRows().Where(rowPredicate).Select(r => r.RowId).ToArray()
            .ShouldBe([101]);
    }

    [Fact]
    public void SemiJoin_string_predicate_uses_dense_string_index_provider_when_available()
    {
        var model = BuildParentChildModel();

        var dense = BuildDenseStringTable(["abc", "zzz"]);

        var parents = InMemoryDb2File.Create(
            tableName: nameof(Parent),
            flags: Db2Flags.None,
            denseStringTableBytes: dense.Bytes,
            rows:
            [
                new InMemoryDb2File.Row(1, [1, 1, "abc"]),
                new InMemoryDb2File.Row(2, [2, 5, "zzz"]),
            ],
            denseStringIndexProvider: dense.OffsetByString);

        var children = InMemoryDb2File.Create(
            tableName: nameof(Child),
            flags: Db2Flags.None,
            denseStringTableBytes: ReadOnlyMemory<byte>.Empty,
            rows:
            [
                new InMemoryDb2File.Row(100, [100, 1, "c1"]),
                new InMemoryDb2File.Row(101, [101, 2, "c2"]),
            ]);

        (IDb2File<RowHandle> File, Db2TableSchema Schema) TableResolver(string tableName)
            => tableName switch
            {
                nameof(Parent) => (parents, SchemaResolver(tableName)),
                nameof(Child) => (children, SchemaResolver(tableName)),
                _ => throw new InvalidOperationException($"Unknown table: {tableName}"),
            };

        Expression<Func<Child, bool>> predicate = c => c.Parent!.Name.Contains("bc");

        Db2NavigationQueryCompiler.TryCompileSemiJoinPredicate(model, children, TableResolver, predicate, out var rowPredicate)
            .ShouldBeTrue();

        var matching = children
            .EnumerateRows()
            .Where(rowPredicate)
            .Select(r => r.RowId)
            .ToArray();

        matching.ShouldBe([100]);
    }

    [Fact]
    public void SemiJoin_collection_count_gt_0_rewrites_to_any_and_matches_principals_with_dependents()
    {
        var model = BuildParentChildCollectionModel();

        var parents = InMemoryDb2File.Create(
            tableName: nameof(Parent),
            flags: Db2Flags.None,
            denseStringTableBytes: ReadOnlyMemory<byte>.Empty,
            rows:
            [
                new InMemoryDb2File.Row(1, [1, 0, "p1"]),
                new InMemoryDb2File.Row(2, [2, 0, "p2"]),
                new InMemoryDb2File.Row(3, [3, 0, "p3"]),
            ]);

        var children = InMemoryDb2File.Create(
            tableName: nameof(Child),
            flags: Db2Flags.None,
            denseStringTableBytes: ReadOnlyMemory<byte>.Empty,
            rows:
            [
                new InMemoryDb2File.Row(100, [100, 1, "c1"]),
                new InMemoryDb2File.Row(101, [101, 1, "c2"]),
                new InMemoryDb2File.Row(102, [102, 0, "c0"]),
            ]);

        (IDb2File<RowHandle> File, Db2TableSchema Schema) TableResolver(string tableName)
            => tableName switch
            {
                nameof(Parent) => (parents, SchemaResolver(tableName)),
                nameof(Child) => (children, SchemaResolver(tableName)),
                _ => throw new InvalidOperationException($"Unknown table: {tableName}"),
            };

        Expression<Func<Parent, bool>> predicate = p => p.Children.Count > 0;

        Db2NavigationQueryCompiler.TryCompileSemiJoinPredicate(model, parents, TableResolver, predicate, out var rowPredicate)
            .ShouldBeTrue();

        var matching = parents
            .EnumerateRows()
            .Where(rowPredicate)
            .Select(r => r.RowId)
            .ToArray();

        matching.ShouldBe([1]);
    }

    [Fact]
    public void SemiJoin_collection_any_with_uncompilable_dependent_predicate_falls_back_to_materializer()
    {
        var model = BuildParentChildCollectionModel();

        var parents = InMemoryDb2File.Create(
            tableName: nameof(Parent),
            flags: Db2Flags.None,
            denseStringTableBytes: ReadOnlyMemory<byte>.Empty,
            rows:
            [
                new InMemoryDb2File.Row(1, [1, 0, "p1"]),
                new InMemoryDb2File.Row(2, [2, 0, "p2"]),
            ]);

        var children = InMemoryDb2File.Create(
            tableName: nameof(Child),
            flags: Db2Flags.None,
            denseStringTableBytes: ReadOnlyMemory<byte>.Empty,
            rows:
            [
                new InMemoryDb2File.Row(100, [100, 1, "alpha"]),
                new InMemoryDb2File.Row(101, [101, 2, "bravo"]),
            ]);

        (IDb2File<RowHandle> File, Db2TableSchema Schema) TableResolver(string tableName)
            => tableName switch
            {
                nameof(Parent) => (parents, SchemaResolver(tableName)),
                nameof(Child) => (children, SchemaResolver(tableName)),
                _ => throw new InvalidOperationException($"Unknown table: {tableName}"),
            };

        var needle = "a";
        Expression<Func<Parent, bool>> predicate = p => p.Children.Any(c => c.Name.Contains(needle));

        Db2NavigationQueryCompiler.TryCompileSemiJoinPredicate(model, parents, TableResolver, predicate, out var rowPredicate)
            .ShouldBeTrue();

        var matching = parents
            .EnumerateRows()
            .Where(rowPredicate)
            .Select(r => r.RowId)
            .ToArray();

        matching.ShouldBe([1, 2]);
    }

    [Fact]
    public void SemiJoin_string_and_scalar_predicates_on_same_navigation_intersect_target_ids()
    {
        var model = BuildParentChildModel();

        var parents = InMemoryDb2File.Create(
            tableName: nameof(Parent),
            flags: Db2Flags.None,
            denseStringTableBytes: ReadOnlyMemory<byte>.Empty,
            rows:
            [
                new InMemoryDb2File.Row(101, [101, 5, "zzz"]),
            ]);

        var children = InMemoryDb2File.Create(
            tableName: nameof(Child),
            flags: Db2Flags.None,
            denseStringTableBytes: ReadOnlyMemory<byte>.Empty,
            rows:
            [
                new InMemoryDb2File.Row(101, [101, 101, "c2"]),
            ]);

        (IDb2File<RowHandle> File, Db2TableSchema Schema) TableResolver(string tableName)
            => tableName switch
            {
                nameof(Parent) => (parents, SchemaResolver(tableName)),
                nameof(Child) => (children, SchemaResolver(tableName)),
                _ => throw new InvalidOperationException($"Unknown table: {tableName}"),
            };

        Expression<Func<Child, bool>> predicate = c => c.Parent!.Name == "zzz" && c.Parent.Level == 5;

        Db2NavigationQueryCompiler.TryCompileSemiJoinPredicate(model, children, TableResolver, predicate, out var rowPredicate)
            .ShouldBeTrue();

        children.EnumerateRows().Where(rowPredicate).Select(r => r.RowId).ToArray()
            .ShouldBe([101]);
    }

    [Fact]
    public void SemiJoin_scalar_and_string_predicates_on_same_navigation_intersect_target_ids()
    {
        var model = BuildParentChildModel();

        var parents = InMemoryDb2File.Create(
            tableName: nameof(Parent),
            flags: Db2Flags.None,
            denseStringTableBytes: ReadOnlyMemory<byte>.Empty,
            rows:
            [
                new InMemoryDb2File.Row(101, [101, 5, "zzz"]),
            ]);

        var children = InMemoryDb2File.Create(
            tableName: nameof(Child),
            flags: Db2Flags.None,
            denseStringTableBytes: ReadOnlyMemory<byte>.Empty,
            rows:
            [
                new InMemoryDb2File.Row(101, [101, 101, "c2"]),
            ]);

        (IDb2File<RowHandle> File, Db2TableSchema Schema) TableResolver(string tableName)
            => tableName switch
            {
                nameof(Parent) => (parents, SchemaResolver(tableName)),
                nameof(Child) => (children, SchemaResolver(tableName)),
                _ => throw new InvalidOperationException($"Unknown table: {tableName}"),
            };

        Expression<Func<Child, bool>> predicate = c => c.Parent!.Level == 5 && c.Parent.Name == "zzz";

        Db2NavigationQueryCompiler.TryCompileSemiJoinPredicate(model, children, TableResolver, predicate, out var rowPredicate)
            .ShouldBeTrue();

        children.EnumerateRows().Where(rowPredicate).Select(r => r.RowId).ToArray()
            .ShouldBe([101]);
    }

    [Fact]
    public void SemiJoin_scalar_and_row_predicates_combine_with_andalso()
    {
        var model = BuildParentChildModel();

        var parents = InMemoryDb2File.Create(
            tableName: nameof(Parent),
            flags: Db2Flags.None,
            denseStringTableBytes: ReadOnlyMemory<byte>.Empty,
            rows:
            [
                new InMemoryDb2File.Row(1, [1, 1, "abc"]),
                new InMemoryDb2File.Row(2, [2, 5, "zzz"]),
            ]);

        var children = InMemoryDb2File.Create(
            tableName: nameof(Child),
            flags: Db2Flags.None,
            denseStringTableBytes: ReadOnlyMemory<byte>.Empty,
            rows:
            [
                new InMemoryDb2File.Row(100, [100, 1, "c1"]),
                new InMemoryDb2File.Row(101, [101, 2, "c2"]),
            ]);

        (IDb2File<RowHandle> File, Db2TableSchema Schema) TableResolver(string tableName)
            => tableName switch
            {
                nameof(Parent) => (parents, SchemaResolver(tableName)),
                nameof(Child) => (children, SchemaResolver(tableName)),
                _ => throw new InvalidOperationException($"Unknown table: {tableName}"),
            };

        Expression<Func<Child, bool>> predicate = c => c.Parent!.Level > 3 && c.Name == "c2";

        Db2NavigationQueryCompiler.TryCompileSemiJoinPredicate(model, children, TableResolver, predicate, out var rowPredicate)
            .ShouldBeTrue();

        children.EnumerateRows().Where(rowPredicate).Select(r => r.RowId).ToArray()
            .ShouldBe([101]);
    }

    [Fact]
    public void SemiJoin_cross_navigation_scalar_and_string_predicates_both_apply()
    {
        var model = BuildChildWithTwoReferencesModel();

        var parents = InMemoryDb2File.Create(
            tableName: nameof(Parent),
            flags: Db2Flags.None,
            denseStringTableBytes: ReadOnlyMemory<byte>.Empty,
            rows:
            [
                new InMemoryDb2File.Row(1, [1, 1, "p1"]),
                new InMemoryDb2File.Row(2, [2, 5, "p2"]),
            ]);

        var categories = InMemoryDb2File.Create(
            tableName: nameof(Category),
            flags: Db2Flags.None,
            denseStringTableBytes: ReadOnlyMemory<byte>.Empty,
            rows:
            [
                new InMemoryDb2File.Row(10, [10, "alpha"]),
                new InMemoryDb2File.Row(20, [20, "bravo"]),
            ]);

        var children = InMemoryDb2File.Create(
            tableName: nameof(ChildWithTwoRefs),
            flags: Db2Flags.None,
            denseStringTableBytes: ReadOnlyMemory<byte>.Empty,
            rows:
            [
                new InMemoryDb2File.Row(100, [100, 1, 10, "c1"]),
                new InMemoryDb2File.Row(101, [101, 2, 20, "c2"]),
            ]);

        (IDb2File<RowHandle> File, Db2TableSchema Schema) TableResolver(string tableName)
            => tableName switch
            {
                nameof(Parent) => (parents, SchemaResolver2(tableName)),
                nameof(Category) => (categories, SchemaResolver2(tableName)),
                nameof(ChildWithTwoRefs) => (children, SchemaResolver2(tableName)),
                _ => throw new InvalidOperationException($"Unknown table: {tableName}"),
            };

        Expression<Func<ChildWithTwoRefs, bool>> predicate = c => c.Parent!.Level == 5 && c.Category!.Name == "bravo";

        Db2NavigationQueryCompiler.TryCompileSemiJoinPredicate(model, children, TableResolver, predicate, out var rowPredicate)
            .ShouldBeTrue();

        children.EnumerateRows().Where(rowPredicate).Select(r => r.RowId).ToArray()
            .ShouldBe([101]);
    }

    [Fact]
    public void SemiJoin_cross_navigation_string_and_scalar_predicates_both_apply()
    {
        var model = BuildChildWithTwoReferencesModel();

        var parents = InMemoryDb2File.Create(
            tableName: nameof(Parent),
            flags: Db2Flags.None,
            denseStringTableBytes: ReadOnlyMemory<byte>.Empty,
            rows:
            [
                new InMemoryDb2File.Row(1, [1, 1, "p1"]),
                new InMemoryDb2File.Row(2, [2, 5, "p2"]),
            ]);

        var categories = InMemoryDb2File.Create(
            tableName: nameof(Category),
            flags: Db2Flags.None,
            denseStringTableBytes: ReadOnlyMemory<byte>.Empty,
            rows:
            [
                new InMemoryDb2File.Row(10, [10, "alpha"]),
                new InMemoryDb2File.Row(20, [20, "bravo"]),
            ]);

        var children = InMemoryDb2File.Create(
            tableName: nameof(ChildWithTwoRefs),
            flags: Db2Flags.None,
            denseStringTableBytes: ReadOnlyMemory<byte>.Empty,
            rows:
            [
                new InMemoryDb2File.Row(100, [100, 1, 10, "c1"]),
                new InMemoryDb2File.Row(101, [101, 2, 20, "c2"]),
            ]);

        (IDb2File<RowHandle> File, Db2TableSchema Schema) TableResolver(string tableName)
            => tableName switch
            {
                nameof(Parent) => (parents, SchemaResolver2(tableName)),
                nameof(Category) => (categories, SchemaResolver2(tableName)),
                nameof(ChildWithTwoRefs) => (children, SchemaResolver2(tableName)),
                _ => throw new InvalidOperationException($"Unknown table: {tableName}"),
            };

        Expression<Func<ChildWithTwoRefs, bool>> predicate = c => c.Category!.Name == "bravo" && c.Parent!.Level == 5;

        Db2NavigationQueryCompiler.TryCompileSemiJoinPredicate(model, children, TableResolver, predicate, out var rowPredicate)
            .ShouldBeTrue();

        children.EnumerateRows().Where(rowPredicate).Select(r => r.RowId).ToArray()
            .ShouldBe([101]);
    }

    [Fact]
    public void SemiJoin_cross_navigation_two_scalar_predicates_both_apply()
    {
        var model = BuildChildWithTwoReferencesModel();

        var parents = InMemoryDb2File.Create(
            tableName: nameof(Parent),
            flags: Db2Flags.None,
            denseStringTableBytes: ReadOnlyMemory<byte>.Empty,
            rows:
            [
                new InMemoryDb2File.Row(1, [1, 1, "p1"]),
                new InMemoryDb2File.Row(2, [2, 5, "p2"]),
            ]);

        var categories = InMemoryDb2File.Create(
            tableName: nameof(Category),
            flags: Db2Flags.None,
            denseStringTableBytes: ReadOnlyMemory<byte>.Empty,
            rows:
            [
                new InMemoryDb2File.Row(10, [10, "alpha"]),
                new InMemoryDb2File.Row(20, [20, "bravo"]),
            ]);

        var children = InMemoryDb2File.Create(
            tableName: nameof(ChildWithTwoRefs),
            flags: Db2Flags.None,
            denseStringTableBytes: ReadOnlyMemory<byte>.Empty,
            rows:
            [
                new InMemoryDb2File.Row(100, [100, 1, 10, "c1"]),
                new InMemoryDb2File.Row(101, [101, 2, 20, "c2"]),
            ]);

        (IDb2File<RowHandle> File, Db2TableSchema Schema) TableResolver(string tableName)
            => tableName switch
            {
                nameof(Parent) => (parents, SchemaResolver2(tableName)),
                nameof(Category) => (categories, SchemaResolver2(tableName)),
                nameof(ChildWithTwoRefs) => (children, SchemaResolver2(tableName)),
                _ => throw new InvalidOperationException($"Unknown table: {tableName}"),
            };

        Expression<Func<ChildWithTwoRefs, bool>> predicate = c => c.Parent!.Level == 5 && c.Category!.Id == 20;

        Db2NavigationQueryCompiler.TryCompileSemiJoinPredicate(model, children, TableResolver, predicate, out var rowPredicate)
            .ShouldBeTrue();

        children.EnumerateRows().Where(rowPredicate).Select(r => r.RowId).ToArray()
            .ShouldBe([101]);
    }

    [Fact]
    public void SemiJoin_two_collection_any_predicates_on_different_navigations_both_apply()
    {
        var model = BuildParentWithTwoCollectionsModel();

        var parents = InMemoryDb2File.Create(
            tableName: nameof(ParentWithTwoCollections),
            flags: Db2Flags.None,
            denseStringTableBytes: ReadOnlyMemory<byte>.Empty,
            rows:
            [
                new InMemoryDb2File.Row(1, [1, 0, "p1"]),
                new InMemoryDb2File.Row(2, [2, 0, "p2"]),
            ]);

        var childrenA = InMemoryDb2File.Create(
            tableName: nameof(ChildA),
            flags: Db2Flags.None,
            denseStringTableBytes: ReadOnlyMemory<byte>.Empty,
            rows:
            [
                new InMemoryDb2File.Row(100, [100, 1, "alpha"]),
                new InMemoryDb2File.Row(101, [101, 2, "nope"]),
            ]);

        var childrenB = InMemoryDb2File.Create(
            tableName: nameof(ChildB),
            flags: Db2Flags.None,
            denseStringTableBytes: ReadOnlyMemory<byte>.Empty,
            rows:
            [
                new InMemoryDb2File.Row(200, [200, 1, "bravo"]),
                new InMemoryDb2File.Row(201, [201, 2, "nope"]),
            ]);

        (IDb2File<RowHandle> File, Db2TableSchema Schema) TableResolver(string tableName)
            => tableName switch
            {
                nameof(ParentWithTwoCollections) => (parents, SchemaResolver3(tableName)),
                nameof(ChildA) => (childrenA, SchemaResolver3(tableName)),
                nameof(ChildB) => (childrenB, SchemaResolver3(tableName)),
                _ => throw new InvalidOperationException($"Unknown table: {tableName}"),
            };

        Expression<Func<ParentWithTwoCollections, bool>> predicate = p =>
            p.ChildrenA.Any(c => c.Id == 100) &&
            p.ChildrenB.Any(c => c.Id == 200);

        Db2NavigationQueryCompiler.TryCompileSemiJoinPredicate(model, parents, TableResolver, predicate, out var rowPredicate)
            .ShouldBeTrue();

        parents.EnumerateRows().Where(rowPredicate).Select(r => r.RowId).ToArray()
            .ShouldBe([1]);
    }

    [Fact]
    public void SemiJoin_null_check_is_null_matches_zero_or_missing_foreign_keys()
    {
        var model = BuildParentChildModel();

        var parents = InMemoryDb2File.Create(
            tableName: nameof(Parent),
            flags: Db2Flags.None,
            denseStringTableBytes: ReadOnlyMemory<byte>.Empty,
            rows:
            [
                new InMemoryDb2File.Row(1, [1, 1, "p1"]),
                new InMemoryDb2File.Row(2, [2, 5, "p2"]),
            ]);

        var children = InMemoryDb2File.Create(
            tableName: nameof(Child),
            flags: Db2Flags.None,
            denseStringTableBytes: ReadOnlyMemory<byte>.Empty,
            rows:
            [
                new InMemoryDb2File.Row(100, [100, 0, "c0"]),
                new InMemoryDb2File.Row(101, [101, 2, "c2"]),
                new InMemoryDb2File.Row(102, [102, 12345, "c-missing"]),
            ]);

        (IDb2File<RowHandle> File, Db2TableSchema Schema) TableResolver(string tableName)
            => tableName switch
            {
                nameof(Parent) => (parents, SchemaResolver(tableName)),
                nameof(Child) => (children, SchemaResolver(tableName)),
                _ => throw new InvalidOperationException($"Unknown table: {tableName}"),
            };

        Expression<Func<Child, bool>> predicate = c => c.Parent == null;

        Db2NavigationQueryCompiler.TryCompileSemiJoinPredicate(model, children, TableResolver, predicate, out var rowPredicate)
            .ShouldBeTrue();

        children.EnumerateRows().Where(rowPredicate).Select(r => r.RowId).ToArray()
            .ShouldBe([100, 102]);
    }

    [Fact]
    public void SemiJoin_null_check_is_not_null_matches_existing_foreign_keys()
    {
        var model = BuildParentChildModel();

        var parents = InMemoryDb2File.Create(
            tableName: nameof(Parent),
            flags: Db2Flags.None,
            denseStringTableBytes: ReadOnlyMemory<byte>.Empty,
            rows:
            [
                new InMemoryDb2File.Row(1, [1, 1, "p1"]),
                new InMemoryDb2File.Row(2, [2, 5, "p2"]),
            ]);

        var children = InMemoryDb2File.Create(
            tableName: nameof(Child),
            flags: Db2Flags.None,
            denseStringTableBytes: ReadOnlyMemory<byte>.Empty,
            rows:
            [
                new InMemoryDb2File.Row(100, [100, 0, "c0"]),
                new InMemoryDb2File.Row(101, [101, 2, "c2"]),
                new InMemoryDb2File.Row(102, [102, 12345, "c-missing"]),
            ]);

        (IDb2File<RowHandle> File, Db2TableSchema Schema) TableResolver(string tableName)
            => tableName switch
            {
                nameof(Parent) => (parents, SchemaResolver(tableName)),
                nameof(Child) => (children, SchemaResolver(tableName)),
                _ => throw new InvalidOperationException($"Unknown table: {tableName}"),
            };

        Expression<Func<Child, bool>> predicate = c => c.Parent != null;

        Db2NavigationQueryCompiler.TryCompileSemiJoinPredicate(model, children, TableResolver, predicate, out var rowPredicate)
            .ShouldBeTrue();

        children.EnumerateRows().Where(rowPredicate).Select(r => r.RowId).ToArray()
            .ShouldBe([101]);
    }

    [Fact]
    public void SemiJoin_string_predicate_startswith_uses_dense_string_index_provider_when_available()
    {
        var model = BuildParentChildModel();

        var dense = BuildDenseStringTable(["alpha", "bravo"]);

        var parents = InMemoryDb2File.Create(
            tableName: nameof(Parent),
            flags: Db2Flags.None,
            denseStringTableBytes: dense.Bytes,
            rows:
            [
                new InMemoryDb2File.Row(1, [1, 1, "alpha"]),
                new InMemoryDb2File.Row(2, [2, 5, "bravo"]),
            ],
            denseStringIndexProvider: dense.OffsetByString);

        var children = InMemoryDb2File.Create(
            tableName: nameof(Child),
            flags: Db2Flags.None,
            denseStringTableBytes: ReadOnlyMemory<byte>.Empty,
            rows:
            [
                new InMemoryDb2File.Row(100, [100, 1, "c1"]),
                new InMemoryDb2File.Row(101, [101, 2, "c2"]),
            ]);

        (IDb2File<RowHandle> File, Db2TableSchema Schema) TableResolver(string tableName)
            => tableName switch
            {
                nameof(Parent) => (parents, SchemaResolver(tableName)),
                nameof(Child) => (children, SchemaResolver(tableName)),
                _ => throw new InvalidOperationException($"Unknown table: {tableName}"),
            };

        Expression<Func<Child, bool>> predicate = c => c.Parent!.Name.StartsWith("al");

        Db2NavigationQueryCompiler.TryCompileSemiJoinPredicate(model, children, TableResolver, predicate, out var rowPredicate)
            .ShouldBeTrue();

        children.EnumerateRows().Where(rowPredicate).Select(r => r.RowId).ToArray()
            .ShouldBe([100]);
    }

    [Fact]
    public void SemiJoin_string_predicate_endswith_falls_back_to_string_ops_when_sparse()
    {
        var model = BuildParentChildModel();

        var dense = BuildDenseStringTable(["alpha", "bravo"]);

        var parents = InMemoryDb2File.Create(
            tableName: nameof(Parent),
            flags: Db2Flags.Sparse,
            denseStringTableBytes: dense.Bytes,
            rows:
            [
                new InMemoryDb2File.Row(1, [1, 1, "alpha"]),
                new InMemoryDb2File.Row(2, [2, 5, "bravo"]),
            ],
            denseStringIndexProvider: dense.OffsetByString);

        var children = InMemoryDb2File.Create(
            tableName: nameof(Child),
            flags: Db2Flags.None,
            denseStringTableBytes: ReadOnlyMemory<byte>.Empty,
            rows:
            [
                new InMemoryDb2File.Row(100, [100, 1, "c1"]),
                new InMemoryDb2File.Row(101, [101, 2, "c2"]),
            ]);

        (IDb2File<RowHandle> File, Db2TableSchema Schema) TableResolver(string tableName)
            => tableName switch
            {
                nameof(Parent) => (parents, SchemaResolver(tableName)),
                nameof(Child) => (children, SchemaResolver(tableName)),
                _ => throw new InvalidOperationException($"Unknown table: {tableName}"),
            };

        Expression<Func<Child, bool>> predicate = c => c.Parent!.Name.EndsWith("vo");

        Db2NavigationQueryCompiler.TryCompileSemiJoinPredicate(model, children, TableResolver, predicate, out var rowPredicate)
            .ShouldBeTrue();

        children.EnumerateRows().Where(rowPredicate).Select(r => r.RowId).ToArray()
            .ShouldBe([101]);
    }

    [Fact]
    public void SemiJoin_scalar_predicate_supports_not_equal_and_less_than_or_equal()
    {
        var model = BuildParentChildModel();

        var parents = InMemoryDb2File.Create(
            tableName: nameof(Parent),
            flags: Db2Flags.None,
            denseStringTableBytes: ReadOnlyMemory<byte>.Empty,
            rows:
            [
                new InMemoryDb2File.Row(1, [1, 1, "p1"]),
                new InMemoryDb2File.Row(2, [2, 5, "p2"]),
            ]);

        var children = InMemoryDb2File.Create(
            tableName: nameof(Child),
            flags: Db2Flags.None,
            denseStringTableBytes: ReadOnlyMemory<byte>.Empty,
            rows:
            [
                new InMemoryDb2File.Row(100, [100, 1, "c1"]),
                new InMemoryDb2File.Row(101, [101, 2, "c2"]),
            ]);

        (IDb2File<RowHandle> File, Db2TableSchema Schema) TableResolver(string tableName)
            => tableName switch
            {
                nameof(Parent) => (parents, SchemaResolver(tableName)),
                nameof(Child) => (children, SchemaResolver(tableName)),
                _ => throw new InvalidOperationException($"Unknown table: {tableName}"),
            };

        Expression<Func<Child, bool>> predicate = c => c.Parent!.Level != 1 && c.Parent.Level <= 5;

        Db2NavigationQueryCompiler.TryCompileSemiJoinPredicate(model, children, TableResolver, predicate, out var rowPredicate)
            .ShouldBeTrue();

        children.EnumerateRows().Where(rowPredicate).Select(r => r.RowId).ToArray()
            .ShouldBe([101]);
    }

    [Fact]
    public void SemiJoin_scalar_predicate_supports_flipped_comparison_constant_on_left()
    {
        var model = BuildParentChildModel();

        var parents = InMemoryDb2File.Create(
            tableName: nameof(Parent),
            flags: Db2Flags.None,
            denseStringTableBytes: ReadOnlyMemory<byte>.Empty,
            rows:
            [
                new InMemoryDb2File.Row(1, [1, 1, "p1"]),
                new InMemoryDb2File.Row(2, [2, 5, "p2"]),
            ]);

        var children = InMemoryDb2File.Create(
            tableName: nameof(Child),
            flags: Db2Flags.None,
            denseStringTableBytes: ReadOnlyMemory<byte>.Empty,
            rows:
            [
                new InMemoryDb2File.Row(100, [100, 1, "c1"]),
                new InMemoryDb2File.Row(101, [101, 2, "c2"]),
            ]);

        (IDb2File<RowHandle> File, Db2TableSchema Schema) TableResolver(string tableName)
            => tableName switch
            {
                nameof(Parent) => (parents, SchemaResolver(tableName)),
                nameof(Child) => (children, SchemaResolver(tableName)),
                _ => throw new InvalidOperationException($"Unknown table: {tableName}"),
            };

        Expression<Func<Child, bool>> predicate = c => 3 < c.Parent!.Level;

        Db2NavigationQueryCompiler.TryCompileSemiJoinPredicate(model, children, TableResolver, predicate, out var rowPredicate)
            .ShouldBeTrue();

        children.EnumerateRows().Where(rowPredicate).Select(r => r.RowId).ToArray()
            .ShouldBe([101]);
    }

    [Fact]
    public void SemiJoin_virtual_foreign_key_field_throws_not_supported()
    {
        var model = BuildParentChildModelVirtualForeignKey();

        var parents = InMemoryDb2File.Create(
            tableName: nameof(Parent),
            flags: Db2Flags.None,
            denseStringTableBytes: ReadOnlyMemory<byte>.Empty,
            rows:
            [
                new InMemoryDb2File.Row(1, [1, 1, "p1"]),
            ]);

        var children = InMemoryDb2File.Create(
            tableName: nameof(Child),
            flags: Db2Flags.None,
            denseStringTableBytes: ReadOnlyMemory<byte>.Empty,
            rows:
            [
                new InMemoryDb2File.Row(100, [100, 1, "c1"]),
            ]);

        (IDb2File<RowHandle> File, Db2TableSchema Schema) TableResolver(string tableName)
            => tableName switch
            {
                nameof(Parent) => (parents, SchemaResolverVirtualForeignKey(tableName)),
                nameof(Child) => (children, SchemaResolverVirtualForeignKey(tableName)),
                _ => throw new InvalidOperationException($"Unknown table: {tableName}"),
            };

        Expression<Func<Child, bool>> predicate = c => c.Parent != null;

        Should.Throw<NotSupportedException>(() =>
        {
            Db2NavigationQueryCompiler.TryCompileSemiJoinPredicate(model, children, TableResolver, predicate, out _);
        });
    }

    [Fact]
    public void SemiJoin_collection_any_and_row_predicates_combine_with_andalso_any_left()
    {
        var model = BuildParentChildCollectionModel();

        var parents = InMemoryDb2File.Create(
            tableName: nameof(Parent),
            flags: Db2Flags.None,
            denseStringTableBytes: ReadOnlyMemory<byte>.Empty,
            rows:
            [
                new InMemoryDb2File.Row(1, [1, 0, "p1"]),
                new InMemoryDb2File.Row(2, [2, 0, "p2"]),
            ]);

        var children = InMemoryDb2File.Create(
            tableName: nameof(Child),
            flags: Db2Flags.None,
            denseStringTableBytes: ReadOnlyMemory<byte>.Empty,
            rows:
            [
                new InMemoryDb2File.Row(100, [100, 1, "c1"]),
                new InMemoryDb2File.Row(101, [101, 2, "c2"]),
            ]);

        (IDb2File<RowHandle> File, Db2TableSchema Schema) TableResolver(string tableName)
            => tableName switch
            {
                nameof(Parent) => (parents, SchemaResolver(tableName)),
                nameof(Child) => (children, SchemaResolver(tableName)),
                _ => throw new InvalidOperationException($"Unknown table: {tableName}"),
            };

        Expression<Func<Parent, bool>> predicate = p => p.Children.Any(c => c.Id == 100) && p.Name == "p1";

        Db2NavigationQueryCompiler.TryCompileSemiJoinPredicate(model, parents, TableResolver, predicate, out var rowPredicate)
            .ShouldBeTrue();

        parents.EnumerateRows().Where(rowPredicate).Select(r => r.RowId).ToArray()
            .ShouldBe([1]);
    }

    [Fact]
    public void SemiJoin_collection_any_and_row_predicates_combine_with_andalso_any_right()
    {
        var model = BuildParentChildCollectionModel();

        var parents = InMemoryDb2File.Create(
            tableName: nameof(Parent),
            flags: Db2Flags.None,
            denseStringTableBytes: ReadOnlyMemory<byte>.Empty,
            rows:
            [
                new InMemoryDb2File.Row(1, [1, 0, "p1"]),
                new InMemoryDb2File.Row(2, [2, 0, "p2"]),
            ]);

        var children = InMemoryDb2File.Create(
            tableName: nameof(Child),
            flags: Db2Flags.None,
            denseStringTableBytes: ReadOnlyMemory<byte>.Empty,
            rows:
            [
                new InMemoryDb2File.Row(100, [100, 1, "c1"]),
                new InMemoryDb2File.Row(101, [101, 2, "c2"]),
            ]);

        (IDb2File<RowHandle> File, Db2TableSchema Schema) TableResolver(string tableName)
            => tableName switch
            {
                nameof(Parent) => (parents, SchemaResolver(tableName)),
                nameof(Child) => (children, SchemaResolver(tableName)),
                _ => throw new InvalidOperationException($"Unknown table: {tableName}"),
            };

        Expression<Func<Parent, bool>> predicate = p => p.Name == "p1" && p.Children.Any(c => c.Id == 100);

        Db2NavigationQueryCompiler.TryCompileSemiJoinPredicate(model, parents, TableResolver, predicate, out var rowPredicate)
            .ShouldBeTrue();

        parents.EnumerateRows().Where(rowPredicate).Select(r => r.RowId).ToArray()
            .ShouldBe([1]);
    }

    [Fact]
    public void SemiJoin_two_collection_any_predicates_on_same_navigation_intersect_matching_ids()
    {
        var model = BuildParentChildCollectionModel();

        var parents = InMemoryDb2File.Create(
            tableName: nameof(Parent),
            flags: Db2Flags.None,
            denseStringTableBytes: ReadOnlyMemory<byte>.Empty,
            rows:
            [
                new InMemoryDb2File.Row(1, [1, 0, "p1"]),
                new InMemoryDb2File.Row(2, [2, 0, "p2"]),
            ]);

        var children = InMemoryDb2File.Create(
            tableName: nameof(Child),
            flags: Db2Flags.None,
            denseStringTableBytes: ReadOnlyMemory<byte>.Empty,
            rows:
            [
                new InMemoryDb2File.Row(100, [100, 1, "alpha"]),
                new InMemoryDb2File.Row(101, [101, 1, "bravo"]),
                new InMemoryDb2File.Row(102, [102, 2, "alpha"]),
            ]);

        (IDb2File<RowHandle> File, Db2TableSchema Schema) TableResolver(string tableName)
            => tableName switch
            {
                nameof(Parent) => (parents, SchemaResolver(tableName)),
                nameof(Child) => (children, SchemaResolver(tableName)),
                _ => throw new InvalidOperationException($"Unknown table: {tableName}"),
            };

        Expression<Func<Parent, bool>> predicate = p =>
            p.Children.Any(c => c.Id == 100) &&
            p.Children.Any(c => c.Name == "alpha");

        Db2NavigationQueryCompiler.TryCompileSemiJoinPredicate(model, parents, TableResolver, predicate, out var rowPredicate)
            .ShouldBeTrue();

        parents.EnumerateRows().Where(rowPredicate).Select(r => r.RowId).ToArray()
            .ShouldBe([1]);
    }

    [Fact]
    public void SemiJoin_string_and_row_predicates_combine_with_andalso_string_left()
    {
        var model = BuildParentChildModel();

        var dense = BuildDenseStringTable(["p1", "p2"]);

        var parents = InMemoryDb2File.Create(
            tableName: nameof(Parent),
            flags: Db2Flags.None,
            denseStringTableBytes: dense.Bytes,
            rows:
            [
                new InMemoryDb2File.Row(1, [1, 1, "p1"]),
                new InMemoryDb2File.Row(2, [2, 5, "p2"]),
            ],
            denseStringIndexProvider: dense.OffsetByString);

        var children = InMemoryDb2File.Create(
            tableName: nameof(Child),
            flags: Db2Flags.None,
            denseStringTableBytes: ReadOnlyMemory<byte>.Empty,
            rows:
            [
                new InMemoryDb2File.Row(100, [100, 1, "c1"]),
                new InMemoryDb2File.Row(101, [101, 2, "c2"]),
            ]);

        (IDb2File<RowHandle> File, Db2TableSchema Schema) TableResolver(string tableName)
            => tableName switch
            {
                nameof(Parent) => (parents, SchemaResolver(tableName)),
                nameof(Child) => (children, SchemaResolver(tableName)),
                _ => throw new InvalidOperationException($"Unknown table: {tableName}"),
            };

        Expression<Func<Child, bool>> predicate = c => c.Parent!.Name.Contains("p") && c.Name == "c2";

        Db2NavigationQueryCompiler.TryCompileSemiJoinPredicate(model, children, TableResolver, predicate, out var rowPredicate)
            .ShouldBeTrue();

        children.EnumerateRows().Where(rowPredicate).Select(r => r.RowId).ToArray()
            .ShouldBe([101]);
    }

    [Fact]
    public void SemiJoin_string_and_row_predicates_combine_with_andalso_string_right()
    {
        var model = BuildParentChildModel();

        var dense = BuildDenseStringTable(["p1", "p2"]);

        var parents = InMemoryDb2File.Create(
            tableName: nameof(Parent),
            flags: Db2Flags.None,
            denseStringTableBytes: dense.Bytes,
            rows:
            [
                new InMemoryDb2File.Row(1, [1, 1, "p1"]),
                new InMemoryDb2File.Row(2, [2, 5, "p2"]),
            ],
            denseStringIndexProvider: dense.OffsetByString);

        var children = InMemoryDb2File.Create(
            tableName: nameof(Child),
            flags: Db2Flags.None,
            denseStringTableBytes: ReadOnlyMemory<byte>.Empty,
            rows:
            [
                new InMemoryDb2File.Row(100, [100, 1, "c1"]),
                new InMemoryDb2File.Row(101, [101, 2, "c2"]),
            ]);

        (IDb2File<RowHandle> File, Db2TableSchema Schema) TableResolver(string tableName)
            => tableName switch
            {
                nameof(Parent) => (parents, SchemaResolver(tableName)),
                nameof(Child) => (children, SchemaResolver(tableName)),
                _ => throw new InvalidOperationException($"Unknown table: {tableName}"),
            };

        Expression<Func<Child, bool>> predicate = c => c.Name == "c2" && c.Parent!.Name.Contains("p");

        Db2NavigationQueryCompiler.TryCompileSemiJoinPredicate(model, children, TableResolver, predicate, out var rowPredicate)
            .ShouldBeTrue();

        children.EnumerateRows().Where(rowPredicate).Select(r => r.RowId).ToArray()
            .ShouldBe([101]);
    }

    [Fact]
    public void SemiJoin_scalar_and_row_predicates_combine_with_andalso_scalar_right()
    {
        var model = BuildParentChildModel();

        var parents = InMemoryDb2File.Create(
            tableName: nameof(Parent),
            flags: Db2Flags.None,
            denseStringTableBytes: ReadOnlyMemory<byte>.Empty,
            rows:
            [
                new InMemoryDb2File.Row(1, [1, 1, "p1"]),
                new InMemoryDb2File.Row(2, [2, 5, "p2"]),
            ]);

        var children = InMemoryDb2File.Create(
            tableName: nameof(Child),
            flags: Db2Flags.None,
            denseStringTableBytes: ReadOnlyMemory<byte>.Empty,
            rows:
            [
                new InMemoryDb2File.Row(100, [100, 1, "c1"]),
                new InMemoryDb2File.Row(101, [101, 2, "c2"]),
            ]);

        (IDb2File<RowHandle> File, Db2TableSchema Schema) TableResolver(string tableName)
            => tableName switch
            {
                nameof(Parent) => (parents, SchemaResolver(tableName)),
                nameof(Child) => (children, SchemaResolver(tableName)),
                _ => throw new InvalidOperationException($"Unknown table: {tableName}"),
            };

        Expression<Func<Child, bool>> predicate = c => c.Name == "c2" && c.Parent!.Level > 3;

        Db2NavigationQueryCompiler.TryCompileSemiJoinPredicate(model, children, TableResolver, predicate, out var rowPredicate)
            .ShouldBeTrue();

        children.EnumerateRows().Where(rowPredicate).Select(r => r.RowId).ToArray()
            .ShouldBe([101]);
    }

    [Fact]
    public void SemiJoin_two_string_predicates_on_same_navigation_intersect_target_ids()
    {
        var model = BuildParentChildModel();

        var dense = BuildDenseStringTable(["p1", "p2"]);

        var parents = InMemoryDb2File.Create(
            tableName: nameof(Parent),
            flags: Db2Flags.None,
            denseStringTableBytes: dense.Bytes,
            rows:
            [
                new InMemoryDb2File.Row(1, [1, 1, "p1"]),
                new InMemoryDb2File.Row(2, [2, 5, "p2"]),
            ],
            denseStringIndexProvider: dense.OffsetByString);

        var children = InMemoryDb2File.Create(
            tableName: nameof(Child),
            flags: Db2Flags.None,
            denseStringTableBytes: ReadOnlyMemory<byte>.Empty,
            rows:
            [
                new InMemoryDb2File.Row(100, [100, 1, "c1"]),
                new InMemoryDb2File.Row(101, [101, 2, "c2"]),
            ]);

        (IDb2File<RowHandle> File, Db2TableSchema Schema) TableResolver(string tableName)
            => tableName switch
            {
                nameof(Parent) => (parents, SchemaResolver(tableName)),
                nameof(Child) => (children, SchemaResolver(tableName)),
                _ => throw new InvalidOperationException($"Unknown table: {tableName}"),
            };

        Expression<Func<Child, bool>> predicate = c => c.Parent!.Name.StartsWith("p") && c.Parent.Name.EndsWith("2");

        Db2NavigationQueryCompiler.TryCompileSemiJoinPredicate(model, children, TableResolver, predicate, out var rowPredicate)
            .ShouldBeTrue();

        children.EnumerateRows().Where(rowPredicate).Select(r => r.RowId).ToArray()
            .ShouldBe([101]);
    }

    [Fact]
    public void SemiJoin_two_scalar_predicates_on_same_navigation_intersect_target_ids()
    {
        var model = BuildParentChildModel();

        var parents = InMemoryDb2File.Create(
            tableName: nameof(Parent),
            flags: Db2Flags.None,
            denseStringTableBytes: ReadOnlyMemory<byte>.Empty,
            rows:
            [
                new InMemoryDb2File.Row(1, [1, 1, "p1"]),
                new InMemoryDb2File.Row(2, [2, 5, "p2"]),
            ]);

        var children = InMemoryDb2File.Create(
            tableName: nameof(Child),
            flags: Db2Flags.None,
            denseStringTableBytes: ReadOnlyMemory<byte>.Empty,
            rows:
            [
                new InMemoryDb2File.Row(100, [100, 1, "c1"]),
                new InMemoryDb2File.Row(101, [101, 2, "c2"]),
            ]);

        (IDb2File<RowHandle> File, Db2TableSchema Schema) TableResolver(string tableName)
            => tableName switch
            {
                nameof(Parent) => (parents, SchemaResolver(tableName)),
                nameof(Child) => (children, SchemaResolver(tableName)),
                _ => throw new InvalidOperationException($"Unknown table: {tableName}"),
            };

        Expression<Func<Child, bool>> predicate = c => c.Parent!.Level > 3 && c.Parent.Level < 10;

        Db2NavigationQueryCompiler.TryCompileSemiJoinPredicate(model, children, TableResolver, predicate, out var rowPredicate)
            .ShouldBeTrue();

        children.EnumerateRows().Where(rowPredicate).Select(r => r.RowId).ToArray()
            .ShouldBe([101]);
    }

    [Fact]
    public void SemiJoin_collection_count_rewrite_supports_not_equal_and_greater_than_or_equal_and_flipped()
    {
        var model = BuildParentChildCollectionModel();

        var parents = InMemoryDb2File.Create(
            tableName: nameof(Parent),
            flags: Db2Flags.None,
            denseStringTableBytes: ReadOnlyMemory<byte>.Empty,
            rows:
            [
                new InMemoryDb2File.Row(1, [1, 0, "p1"]),
                new InMemoryDb2File.Row(2, [2, 0, "p2"]),
            ]);

        var children = InMemoryDb2File.Create(
            tableName: nameof(Child),
            flags: Db2Flags.None,
            denseStringTableBytes: ReadOnlyMemory<byte>.Empty,
            rows:
            [
                new InMemoryDb2File.Row(100, [100, 1, "c1"]),
                new InMemoryDb2File.Row(101, [101, 1, "c2"]),
                new InMemoryDb2File.Row(102, [102, 0, "c0"]),
            ]);

        (IDb2File<RowHandle> File, Db2TableSchema Schema) TableResolver(string tableName)
            => tableName switch
            {
                nameof(Parent) => (parents, SchemaResolver(tableName)),
                nameof(Child) => (children, SchemaResolver(tableName)),
                _ => throw new InvalidOperationException($"Unknown table: {tableName}"),
            };

        Expression<Func<Parent, bool>> predicate1 = p => p.Children.Count != 0;
        Db2NavigationQueryCompiler.TryCompileSemiJoinPredicate(model, parents, TableResolver, predicate1, out var rowPredicate1)
            .ShouldBeTrue();

        parents.EnumerateRows().Where(rowPredicate1).Select(r => r.RowId).ToArray()
            .ShouldBe([1]);

        Expression<Func<Parent, bool>> predicate2 = p => p.Children.Count >= 1;
        Db2NavigationQueryCompiler.TryCompileSemiJoinPredicate(model, parents, TableResolver, predicate2, out var rowPredicate2)
            .ShouldBeTrue();

        parents.EnumerateRows().Where(rowPredicate2).Select(r => r.RowId).ToArray()
            .ShouldBe([1]);

        Expression<Func<Parent, bool>> predicate3 = p => 0 < p.Children.Count;
        Db2NavigationQueryCompiler.TryCompileSemiJoinPredicate(model, parents, TableResolver, predicate3, out var rowPredicate3)
            .ShouldBeTrue();

        parents.EnumerateRows().Where(rowPredicate3).Select(r => r.RowId).ToArray()
            .ShouldBe([1]);
    }

    [Fact]
    public void SemiJoin_shared_primary_key_one_to_one_uses_row_id_matching()
    {
        var model = BuildSharedPrimaryKeyModel();

        var parents = InMemoryDb2File.Create(
            tableName: nameof(SpkParent),
            flags: Db2Flags.None,
            denseStringTableBytes: ReadOnlyMemory<byte>.Empty,
            rows:
            [
                new InMemoryDb2File.Row(1, [1, "alpha"]),
                new InMemoryDb2File.Row(2, [2, "bravo"]),
            ]);

        var children = InMemoryDb2File.Create(
            tableName: nameof(SpkChild),
            flags: Db2Flags.None,
            denseStringTableBytes: ReadOnlyMemory<byte>.Empty,
            rows:
            [
                new InMemoryDb2File.Row(1, [1, "c1"]),
                new InMemoryDb2File.Row(2, [2, "c2"]),
            ]);

        (IDb2File<RowHandle> File, Db2TableSchema Schema) TableResolver(string tableName)
            => tableName switch
            {
                nameof(SpkParent) => (parents, SchemaResolverSpk(tableName)),
                nameof(SpkChild) => (children, SchemaResolverSpk(tableName)),
                _ => throw new InvalidOperationException($"Unknown table: {tableName}"),
            };

        Expression<Func<SpkChild, bool>> predicate = c => c.Parent!.Name == "alpha";

        Db2NavigationQueryCompiler.TryCompileSemiJoinPredicate(model, children, TableResolver, predicate, out var rowPredicate)
            .ShouldBeTrue();

        children.EnumerateRows().Where(rowPredicate).Select(r => r.RowId).ToArray()
            .ShouldBe([1]);
    }

    private static Db2Model BuildParentChildModel()
    {
        var builder = new Db2ModelBuilder();

        builder.Entity<Child>()
            .HasOne(x => x.Parent)
            .WithForeignKey(x => x.ParentId);

        builder.Entity<Parent>().HasKey(x => x.Id);
        builder.Entity<Child>().HasKey(x => x.Id);

        return builder.Build(SchemaResolver);
    }

    private static Db2Model BuildParentChildModelVirtualForeignKey()
    {
        var builder = new Db2ModelBuilder();

        builder.Entity<Child>()
            .HasOne(x => x.Parent)
            .WithForeignKey(x => x.ParentId);

        builder.Entity<Parent>().HasKey(x => x.Id);
        builder.Entity<Child>().HasKey(x => x.Id);

        return builder.Build(SchemaResolverVirtualForeignKey);
    }

    private static Db2Model BuildSharedPrimaryKeyModel()
    {
        var builder = new Db2ModelBuilder();

        builder.Entity<SpkChild>()
            .HasOne(x => x.Parent)
            .WithSharedPrimaryKey(x => x.Id, x => x.Id);

        builder.Entity<SpkParent>().HasKey(x => x.Id);
        builder.Entity<SpkChild>().HasKey(x => x.Id);

        return builder.Build(SchemaResolverSpk);
    }

    private static Db2Model BuildParentChildCollectionModel()
    {
        var builder = new Db2ModelBuilder();

        builder.Entity<Parent>()
            .HasMany(x => x.Children)
            .WithForeignKey(x => x.ParentId);

        builder.Entity<Parent>().HasKey(x => x.Id);
        builder.Entity<Child>().HasKey(x => x.Id);

        return builder.Build(SchemaResolver);
    }

    private static Db2TableSchema SchemaResolver(string tableName)
    {
        return tableName switch
        {
            nameof(Parent) => new Db2TableSchema(
                tableName: nameof(Parent),
                layoutHash: 0,
                physicalColumnCount: 3,
                fields:
                [
                    new Db2FieldSchema("ID", Db2ValueType.Int64, ColumnStartIndex: 0, ElementCount: 1, IsVerified: true, IsVirtual: false, IsId: true, IsRelation: false, ReferencedTableName: null),
                    new Db2FieldSchema(nameof(Parent.Level), Db2ValueType.Int64, ColumnStartIndex: 1, ElementCount: 1, IsVerified: true, IsVirtual: false, IsId: false, IsRelation: false, ReferencedTableName: null),
                    new Db2FieldSchema(nameof(Parent.Name), Db2ValueType.String, ColumnStartIndex: 2, ElementCount: 1, IsVerified: true, IsVirtual: false, IsId: false, IsRelation: false, ReferencedTableName: null),
                ]),

            nameof(Child) => new Db2TableSchema(
                tableName: nameof(Child),
                layoutHash: 0,
                physicalColumnCount: 3,
                fields:
                [
                    new Db2FieldSchema("ID", Db2ValueType.Int64, ColumnStartIndex: 0, ElementCount: 1, IsVerified: true, IsVirtual: false, IsId: true, IsRelation: false, ReferencedTableName: null),
                    new Db2FieldSchema(nameof(Child.ParentId), Db2ValueType.Int64, ColumnStartIndex: 1, ElementCount: 1, IsVerified: true, IsVirtual: false, IsId: false, IsRelation: false, ReferencedTableName: nameof(Parent)),
                    new Db2FieldSchema(nameof(Child.Name), Db2ValueType.String, ColumnStartIndex: 2, ElementCount: 1, IsVerified: true, IsVirtual: false, IsId: false, IsRelation: false, ReferencedTableName: null),
                ]),

            _ => throw new InvalidOperationException($"Unknown table: {tableName}"),
        };
    }

    private static Db2TableSchema SchemaResolverVirtualForeignKey(string tableName)
    {
        return tableName switch
        {
            nameof(Parent) => new Db2TableSchema(
                tableName: nameof(Parent),
                layoutHash: 0,
                physicalColumnCount: 3,
                fields:
                [
                    new Db2FieldSchema("ID", Db2ValueType.Int64, ColumnStartIndex: 0, ElementCount: 1, IsVerified: true, IsVirtual: false, IsId: true, IsRelation: false, ReferencedTableName: null),
                    new Db2FieldSchema(nameof(Parent.Level), Db2ValueType.Int64, ColumnStartIndex: 1, ElementCount: 1, IsVerified: true, IsVirtual: false, IsId: false, IsRelation: false, ReferencedTableName: null),
                    new Db2FieldSchema(nameof(Parent.Name), Db2ValueType.String, ColumnStartIndex: 2, ElementCount: 1, IsVerified: true, IsVirtual: false, IsId: false, IsRelation: false, ReferencedTableName: null),
                ]),

            nameof(Child) => new Db2TableSchema(
                tableName: nameof(Child),
                layoutHash: 0,
                physicalColumnCount: 3,
                fields:
                [
                    new Db2FieldSchema("ID", Db2ValueType.Int64, ColumnStartIndex: 0, ElementCount: 1, IsVerified: true, IsVirtual: false, IsId: true, IsRelation: false, ReferencedTableName: null),
                    new Db2FieldSchema(nameof(Child.ParentId), Db2ValueType.Int64, ColumnStartIndex: 1, ElementCount: 1, IsVerified: true, IsVirtual: true, IsId: false, IsRelation: false, ReferencedTableName: nameof(Parent)),
                    new Db2FieldSchema(nameof(Child.Name), Db2ValueType.String, ColumnStartIndex: 2, ElementCount: 1, IsVerified: true, IsVirtual: false, IsId: false, IsRelation: false, ReferencedTableName: null),
                ]),

            _ => throw new InvalidOperationException($"Unknown table: {tableName}"),
        };
    }

    private static Db2TableSchema SchemaResolverSpk(string tableName)
    {
        return tableName switch
        {
            nameof(SpkParent) => new Db2TableSchema(
                tableName: nameof(SpkParent),
                layoutHash: 0,
                physicalColumnCount: 2,
                fields:
                [
                    new Db2FieldSchema("ID", Db2ValueType.Int64, ColumnStartIndex: 0, ElementCount: 1, IsVerified: true, IsVirtual: false, IsId: true, IsRelation: false, ReferencedTableName: null),
                    new Db2FieldSchema(nameof(SpkParent.Name), Db2ValueType.String, ColumnStartIndex: 1, ElementCount: 1, IsVerified: true, IsVirtual: false, IsId: false, IsRelation: false, ReferencedTableName: null),
                ]),

            nameof(SpkChild) => new Db2TableSchema(
                tableName: nameof(SpkChild),
                layoutHash: 0,
                physicalColumnCount: 2,
                fields:
                [
                    new Db2FieldSchema("ID", Db2ValueType.Int64, ColumnStartIndex: 0, ElementCount: 1, IsVerified: true, IsVirtual: false, IsId: true, IsRelation: false, ReferencedTableName: null),
                    new Db2FieldSchema(nameof(SpkChild.Name), Db2ValueType.String, ColumnStartIndex: 1, ElementCount: 1, IsVerified: true, IsVirtual: false, IsId: false, IsRelation: false, ReferencedTableName: null),
                ]),

            _ => throw new InvalidOperationException($"Unknown table: {tableName}"),
        };
    }

    private static (ReadOnlyMemory<byte> Bytes, IReadOnlyDictionary<string, int> OffsetByString) BuildDenseStringTable(string[] strings)
    {
        var offsetByString = new Dictionary<string, int>(StringComparer.Ordinal);
        var bytes = new List<byte>(capacity: strings.Sum(s => s.Length + 1));

        foreach (var s in strings)
        {
            offsetByString[s] = bytes.Count;
            bytes.AddRange(System.Text.Encoding.UTF8.GetBytes(s));
            bytes.Add(0);
        }

        return (Bytes: bytes.ToArray(), OffsetByString: offsetByString);
    }

    private sealed class Parent
    {
        public int Id { get; set; }

        public int Level { get; set; }

        public string Name { get; set; } = string.Empty;

        public ICollection<Child> Children { get; set; } = [];
    }

    private sealed class Child
    {
        public int Id { get; set; }

        public int ParentId { get; set; }

        public string Name { get; set; } = string.Empty;

        public Parent? Parent { get; set; }
    }

    private sealed class Category
    {
        public int Id { get; set; }

        public string Name { get; set; } = string.Empty;
    }

    private sealed class ChildWithTwoRefs
    {
        public int Id { get; set; }

        public int ParentId { get; set; }

        public int CategoryId { get; set; }

        public string Name { get; set; } = string.Empty;

        public Parent? Parent { get; set; }

        public Category? Category { get; set; }
    }

    private sealed class ParentWithTwoCollections
    {
        public int Id { get; set; }

        public string Name { get; set; } = string.Empty;

        public ICollection<ChildA> ChildrenA { get; set; } = [];

        public ICollection<ChildB> ChildrenB { get; set; } = [];
    }

    private sealed class ChildA
    {
        public int Id { get; set; }

        public int ParentId { get; set; }

        public string Name { get; set; } = string.Empty;
    }

    private sealed class ChildB
    {
        public int Id { get; set; }

        public int ParentId { get; set; }

        public string Name { get; set; } = string.Empty;
    }

    private sealed class SpkParent
    {
        public int Id { get; set; }

        public string Name { get; set; } = string.Empty;
    }

    private sealed class SpkChild
    {
        public int Id { get; set; }

        public string Name { get; set; } = string.Empty;

        public SpkParent? Parent { get; set; }
    }

    private sealed class InMemoryDb2File(string tableName,
        Db2Flags flags,
        ReadOnlyMemory<byte> denseStringTableBytes,
        IReadOnlyDictionary<string, int>? denseStringIndexProvider,
        IReadOnlyDictionary<int, object[]> valuesByRowId) : IDb2File<RowHandle>, IDb2DenseStringTableIndexProvider<RowHandle>
    {
        public string TableName { get; } = tableName;

        public Type RowType { get; } = typeof(RowHandle);

        public Db2Flags Flags { get; } = flags;

        public int RecordsCount { get; } = valuesByRowId.Count;

        public ReadOnlyMemory<byte> DenseStringTableBytes { get; } = denseStringTableBytes;

        public IEnumerable<RowHandle> EnumerateRowHandles() => EnumerateRows();

        public IEnumerable<RowHandle> EnumerateRows()
            => valuesByRowId.Keys.OrderBy(k => k).Select((id, i) => new RowHandle(0, i, id));

        public T ReadField<T>(RowHandle handle, int fieldIndex)
        {
            var values = valuesByRowId[handle.RowId];
            return (T)values[fieldIndex];
        }

        public bool TryGetRowHandle<TId>(TId id, out RowHandle handle) where TId : IEquatable<TId>, IComparable<TId>
        {
            if (id is not int rowId)
                throw new NotSupportedException($"Only int IDs are supported by {nameof(InMemoryDb2File)}.");

            if (valuesByRowId.ContainsKey(rowId))
            {
                handle = new RowHandle(0, 0, rowId);
                return true;
            }

            handle = default;
            return false;
        }

        public bool TryGetRowById<TId>(TId id, out RowHandle row) where TId : IEquatable<TId>, IComparable<TId>
        {
            if (TryGetRowHandle(id, out var handle))
            {
                row = handle;
                return true;
            }

            row = default;
            return false;
        }

        public bool TryGetDenseStringTableIndex(RowHandle row, int fieldIndex, out int stringTableIndex)
        {
            stringTableIndex = 0;

            if (denseStringIndexProvider is null)
                return false;

            var value = ReadField<string>(row, fieldIndex);
            return denseStringIndexProvider.TryGetValue(value, out stringTableIndex);
        }

        public static InMemoryDb2File Create(
            string tableName,
            Db2Flags flags,
            ReadOnlyMemory<byte> denseStringTableBytes,
            IReadOnlyList<Row> rows,
            IReadOnlyDictionary<string, int>? denseStringIndexProvider = null)
        {
            var valuesByRowId = rows.ToDictionary(r => r.Id, r => r.Values);

            return new InMemoryDb2File(
                tableName,
                flags,
                denseStringTableBytes,
                denseStringIndexProvider,
                valuesByRowId);
        }

        public readonly record struct Row(int Id, object[] Values);
    }

    private static Db2Model BuildChildWithTwoReferencesModel()
    {
        var builder = new Db2ModelBuilder();

        builder.Entity<ChildWithTwoRefs>()
            .HasOne(x => x.Parent)
            .WithForeignKey(x => x.ParentId);

        builder.Entity<ChildWithTwoRefs>()
            .HasOne(x => x.Category)
            .WithForeignKey(x => x.CategoryId);

        builder.Entity<Parent>().HasKey(x => x.Id);
        builder.Entity<Category>().HasKey(x => x.Id);
        builder.Entity<ChildWithTwoRefs>().HasKey(x => x.Id);

        return builder.Build(SchemaResolver2);
    }

    private static Db2TableSchema SchemaResolver2(string tableName)
    {
        return tableName switch
        {
            nameof(Parent) => new Db2TableSchema(
                tableName: nameof(Parent),
                layoutHash: 0,
                physicalColumnCount: 3,
                fields:
                [
                    new Db2FieldSchema("ID", Db2ValueType.Int64, ColumnStartIndex: 0, ElementCount: 1, IsVerified: true, IsVirtual: false, IsId: true, IsRelation: false, ReferencedTableName: null),
                    new Db2FieldSchema(nameof(Parent.Level), Db2ValueType.Int64, ColumnStartIndex: 1, ElementCount: 1, IsVerified: true, IsVirtual: false, IsId: false, IsRelation: false, ReferencedTableName: null),
                    new Db2FieldSchema(nameof(Parent.Name), Db2ValueType.String, ColumnStartIndex: 2, ElementCount: 1, IsVerified: true, IsVirtual: false, IsId: false, IsRelation: false, ReferencedTableName: null),
                ]),

            nameof(Category) => new Db2TableSchema(
                tableName: nameof(Category),
                layoutHash: 0,
                physicalColumnCount: 2,
                fields:
                [
                    new Db2FieldSchema("ID", Db2ValueType.Int64, ColumnStartIndex: 0, ElementCount: 1, IsVerified: true, IsVirtual: false, IsId: true, IsRelation: false, ReferencedTableName: null),
                    new Db2FieldSchema(nameof(Category.Name), Db2ValueType.String, ColumnStartIndex: 1, ElementCount: 1, IsVerified: true, IsVirtual: false, IsId: false, IsRelation: false, ReferencedTableName: null),
                ]),

            nameof(ChildWithTwoRefs) => new Db2TableSchema(
                tableName: nameof(ChildWithTwoRefs),
                layoutHash: 0,
                physicalColumnCount: 4,
                fields:
                [
                    new Db2FieldSchema("ID", Db2ValueType.Int64, ColumnStartIndex: 0, ElementCount: 1, IsVerified: true, IsVirtual: false, IsId: true, IsRelation: false, ReferencedTableName: null),
                    new Db2FieldSchema(nameof(ChildWithTwoRefs.ParentId), Db2ValueType.Int64, ColumnStartIndex: 1, ElementCount: 1, IsVerified: true, IsVirtual: false, IsId: false, IsRelation: false, ReferencedTableName: nameof(Parent)),
                    new Db2FieldSchema(nameof(ChildWithTwoRefs.CategoryId), Db2ValueType.Int64, ColumnStartIndex: 2, ElementCount: 1, IsVerified: true, IsVirtual: false, IsId: false, IsRelation: false, ReferencedTableName: nameof(Category)),
                    new Db2FieldSchema(nameof(ChildWithTwoRefs.Name), Db2ValueType.String, ColumnStartIndex: 3, ElementCount: 1, IsVerified: true, IsVirtual: false, IsId: false, IsRelation: false, ReferencedTableName: null),
                ]),

            _ => throw new InvalidOperationException($"Unknown table: {tableName}"),
        };
    }

    private static Db2Model BuildParentWithTwoCollectionsModel()
    {
        var builder = new Db2ModelBuilder();

        builder.Entity<ParentWithTwoCollections>()
            .HasMany(x => x.ChildrenA)
            .WithForeignKey(x => x.ParentId);

        builder.Entity<ParentWithTwoCollections>()
            .HasMany(x => x.ChildrenB)
            .WithForeignKey(x => x.ParentId);

        builder.Entity<ParentWithTwoCollections>().HasKey(x => x.Id);
        builder.Entity<ChildA>().HasKey(x => x.Id);
        builder.Entity<ChildB>().HasKey(x => x.Id);

        return builder.Build(SchemaResolver3);
    }

    private static Db2TableSchema SchemaResolver3(string tableName)
    {
        return tableName switch
        {
            nameof(ParentWithTwoCollections) => new Db2TableSchema(
                tableName: nameof(ParentWithTwoCollections),
                layoutHash: 0,
                physicalColumnCount: 3,
                fields:
                [
                    new Db2FieldSchema("ID", Db2ValueType.Int64, ColumnStartIndex: 0, ElementCount: 1, IsVerified: true, IsVirtual: false, IsId: true, IsRelation: false, ReferencedTableName: null),
                    new Db2FieldSchema("Unused", Db2ValueType.Int64, ColumnStartIndex: 1, ElementCount: 1, IsVerified: true, IsVirtual: false, IsId: false, IsRelation: false, ReferencedTableName: null),
                    new Db2FieldSchema(nameof(ParentWithTwoCollections.Name), Db2ValueType.String, ColumnStartIndex: 2, ElementCount: 1, IsVerified: true, IsVirtual: false, IsId: false, IsRelation: false, ReferencedTableName: null),
                ]),

            nameof(ChildA) => new Db2TableSchema(
                tableName: nameof(ChildA),
                layoutHash: 0,
                physicalColumnCount: 3,
                fields:
                [
                    new Db2FieldSchema("ID", Db2ValueType.Int64, ColumnStartIndex: 0, ElementCount: 1, IsVerified: true, IsVirtual: false, IsId: true, IsRelation: false, ReferencedTableName: null),
                    new Db2FieldSchema(nameof(ChildA.ParentId), Db2ValueType.Int64, ColumnStartIndex: 1, ElementCount: 1, IsVerified: true, IsVirtual: false, IsId: false, IsRelation: false, ReferencedTableName: nameof(ParentWithTwoCollections)),
                    new Db2FieldSchema(nameof(ChildA.Name), Db2ValueType.String, ColumnStartIndex: 2, ElementCount: 1, IsVerified: true, IsVirtual: false, IsId: false, IsRelation: false, ReferencedTableName: null),
                ]),

            nameof(ChildB) => new Db2TableSchema(
                tableName: nameof(ChildB),
                layoutHash: 0,
                physicalColumnCount: 3,
                fields:
                [
                    new Db2FieldSchema("ID", Db2ValueType.Int64, ColumnStartIndex: 0, ElementCount: 1, IsVerified: true, IsVirtual: false, IsId: true, IsRelation: false, ReferencedTableName: null),
                    new Db2FieldSchema(nameof(ChildB.ParentId), Db2ValueType.Int64, ColumnStartIndex: 1, ElementCount: 1, IsVerified: true, IsVirtual: false, IsId: false, IsRelation: false, ReferencedTableName: nameof(ParentWithTwoCollections)),
                    new Db2FieldSchema(nameof(ChildB.Name), Db2ValueType.String, ColumnStartIndex: 2, ElementCount: 1, IsVerified: true, IsVirtual: false, IsId: false, IsRelation: false, ReferencedTableName: null),
                ]),

            _ => throw new InvalidOperationException($"Unknown table: {tableName}"),
        };
    }
}
