using System.Linq.Expressions;
using System.Reflection;

using MimironSQL.Db2;
using MimironSQL.Db2.Model;
using MimironSQL.Db2.Query;
using MimironSQL.Db2.Schema;
using MimironSQL.Formats;

using Shouldly;

namespace MimironSQL.Tests;

public sealed class Db2QueryProviderTests
{
    [Fact]
    public void ExecuteScalar_supports_Any_Count_and_All()
    {
        var (table, _) = CreateParentTable();

        table.Any().ShouldBeTrue();
        table.Count().ShouldBe(3);

        table.Where(x => x.Level > 0).Any().ShouldBeTrue();
        table.Where(x => x.Level > 0).Count().ShouldBe(2);

        table.All(x => x.Level >= 0).ShouldBeTrue();
        table.All(x => x.Level > 0).ShouldBeFalse();
    }

    [Fact]
    public void ExecuteScalar_supports_First_FirstOrDefault_Single_and_SingleOrDefault()
    {
        var (table, _) = CreateParentTable();

        table.Where(x => x.Id == 2).First().Id.ShouldBe(2);
        table.Where(x => x.Id == 999).FirstOrDefault().ShouldBeNull();

        table.Where(x => x.Id == 1).Single().Id.ShouldBe(1);
        table.Where(x => x.Id == 999).SingleOrDefault().ShouldBeNull();

        Should.Throw<InvalidOperationException>(() => table.Where(x => x.Id == 999).First());
        Should.Throw<InvalidOperationException>(() => table.Where(x => x.Level >= 0).Single());
    }

    [Fact]
    public void ExecuteScalar_throws_when_no_terminal_operator_or_result_type_mismatch()
    {
        var (table, provider) = CreateParentTable();

        var noTerminal = Should.Throw<NotSupportedException>(() => provider.Execute<int>(table.Expression));
        noTerminal.Message.ShouldContain("terminal operator");

        var countCall = Expression.Call(
            typeof(Queryable),
            nameof(Queryable.Count),
            [typeof(Parent)],
            table.Expression);

        var mismatch = Should.Throw<NotSupportedException>(() => provider.Execute<bool>(countCall));
        mismatch.Message.ShouldContain("expected result type");
    }

    [Fact]
    public void Execute_generic_does_not_support_IQueryable_results()
    {
        var (table, provider) = CreateParentTable();

        var ex = Should.Throw<NotSupportedException>(() => provider.Execute<IQueryable<Parent>>(table.Expression));
        ex.Message.ShouldContain("IQueryable");
    }

    [Fact]
    public void CreateQuery_non_generic_creates_a_queryable_and_executes()
    {
        var (table, provider) = CreateParentTable();

        Expression<Func<Parent, bool>> predicate = p => p.Level > 0;

        var whereCall = Expression.Call(
            typeof(Queryable),
            nameof(Queryable.Where),
            [typeof(Parent)],
            table.Expression,
            Expression.Quote(predicate));

        var query = ((IQueryProvider)provider).CreateQuery(whereCall);

        query.ElementType.ShouldBe(typeof(Parent));

        List<int> ids = [];
        foreach (var item in query)
            ids.Add(((Parent)item).Id);

        ids.OrderBy(x => x).ToArray().ShouldBe([1, 2]);
    }

    [Fact]
    public void Execute_non_generic_executes_for_enumerable_queries()
    {
        var (table, provider) = CreateParentTable();

        var query = table.Where(p => p.Level > 0);

        var obj = ((IQueryProvider)provider).Execute(query.Expression);
        obj.ShouldNotBeNull();

        var items = (IEnumerable<Parent>)obj!;
        items.Select(x => x.Id).OrderBy(x => x).ToArray().ShouldBe([1, 2]);
    }

    [Fact]
    public void Execute_non_generic_throws_for_scalar_expressions_and_string_is_not_treated_as_enumerable()
    {
        var (_, provider) = CreateParentTable();

        Should.Throw<NotSupportedException>(() => ((IQueryProvider)provider).Execute(Expression.Constant(123)));
        Should.Throw<NotSupportedException>(() => ((IQueryProvider)provider).Execute(Expression.Constant("hello")));
    }

    [Fact]
    public void Where_suppresses_NullReferenceException_in_entity_predicates()
    {
        var (table, _) = CreateParentTable();

        table.Where(p => AlwaysThrowsNullRef(p)).ToArray().ShouldBeEmpty();
        table.Where(p => p.Level > 0).Where(p => AlwaysThrowsNullRef(p)).ToArray().ShouldBeEmpty();
    }

    [Fact]
    public void Select_and_Take_can_prune_to_row_level_reads()
    {
        var (table, _) = CreateParentTable();

        var result = table
            .Where(p => p.Level > 0)
            .Select(p => p.Level)
            .Take(1)
            .ToArray();

        result.Length.ShouldBe(1);
        result[0].ShouldBe(1);
    }

    [Fact]
    public void Include_reference_navigation_sets_related_entity_by_foreign_key()
    {
        var (children, _, _) = CreateParentChildTables();

        var result = children
            .Include(c => c.Parent)
            .ToArray();

        result.Length.ShouldBe(3);

        result.Single(x => x.Id == 10).Parent.ShouldNotBeNull();
        result.Single(x => x.Id == 10).Parent!.Name.ShouldBe("p1");

        result.Single(x => x.Id == 11).Parent.ShouldNotBeNull();
        result.Single(x => x.Id == 11).Parent!.Name.ShouldBe("p2");

        result.Single(x => x.Id == 12).Parent.ShouldBeNull();
    }

    [Fact]
    public void Include_collection_navigation_populates_dependents_by_foreign_key()
    {
        var (_, parents, _) = CreateParentChildTables();

        var result = parents
            .Include(p => p.Children)
            .ToArray()
            .OrderBy(p => p.Id)
            .ToArray();

        result.Length.ShouldBe(2);

        var p1 = result[0];
        p1.Id.ShouldBe(1);
        p1.Children.ShouldBeOfType<Child[]>();
        ((Child[])p1.Children).Select(c => c.Id).OrderBy(x => x).ToArray().ShouldBe([10]);

        var p2 = result[1];
        p2.Id.ShouldBe(2);
        p2.Children.ShouldBeOfType<Child[]>();
        ((Child[])p2.Children).Select(c => c.Id).OrderBy(x => x).ToArray().ShouldBe([11]);
    }

    [Fact]
    public void Include_collection_navigation_foreign_key_array_preserves_id_order()
    {
        var (parentWithIds, _, _) = CreateParentWithChildIdsTables();

        var result = parentWithIds
            .Include(p => p.Children)
            .ToArray();

        result.Length.ShouldBe(1);

        var arr = (Child[])result[0].Children;
        arr.Length.ShouldBe(3);
        arr[0].Id.ShouldBe(10);
        arr[1].ShouldBeNull();
        arr[2].Id.ShouldBe(11);
    }

    [Fact]
    public void Navigation_projection_pruning_can_apply_semi_join_predicates_and_project_related_values()
    {
        var (children, _, _) = CreateParentChildTables();

        var results = children
            .Include(c => c.Parent)
            .Where(c => c.Parent!.Level > 3)
            .Select(c => c.Parent!.Name)
            .ToArray();

        results.ShouldBe(["p2"]);
    }

    [Fact]
    public void Navigation_access_with_multiple_Selects_includes_navigation_then_projects()
    {
        var (children, _, _) = CreateParentChildTables();

        var results = children
            .Include(c => c.Parent)
            .Where(c => c.ParentId != 0)
            .Select(c => c.Parent!.Name)
            .Select(name => name.Length)
            .ToArray();

        results.ShouldBe([2, 2]);
    }

    [Fact]
    public void Include_after_Select_that_changes_element_type_throws()
    {
        var (children, _, _) = CreateParentChildTables();

        var ex = Should.Throw<NotSupportedException>(() =>
            children
                .Select(c => c.Parent!)
                .Include(p => p.Children)
                .ToArray());

        ex.Message.ShouldContain("Include");
        ex.Message.ShouldContain("Select");
    }

    [Fact]
    public void CreateIntEnumerableGetter_converts_nullable_enum_arrays_without_boxing()
    {
        var member = typeof(EntityWithNullableEnumKeys).GetProperty(nameof(EntityWithNullableEnumKeys.Keys))!;

        var executorType = typeof(Db2IncludeChainExecutor);
        var method = executorType.GetMethod("GetOrCreateIntEnumerableGetter", System.Reflection.BindingFlags.Static | System.Reflection.BindingFlags.NonPublic)!;

        var getter = (Func<object, IEnumerable<int>?>)method.Invoke(null, [typeof(EntityWithNullableEnumKeys), member])!;

        var entity = new EntityWithNullableEnumKeys
        {
            Keys = [TestKey.Ten, null, TestKey.Eleven],
        };

        getter(entity)!.ToArray().ShouldBe([10, 0, 11]);
    }

    [Fact]
    public void CreateIntEnumerableGetter_throws_for_string_enumerables_even_though_string_is_IEnumerable()
    {
        var member = typeof(EntityWithStringKeys).GetProperty(nameof(EntityWithStringKeys.Keys))!;

        var executorType = typeof(Db2IncludeChainExecutor);
        var method = executorType.GetMethod("GetOrCreateIntEnumerableGetter", System.Reflection.BindingFlags.Static | System.Reflection.BindingFlags.NonPublic)!;

        var ex = Should.Throw<TargetInvocationException>(() => method.Invoke(null, [typeof(EntityWithStringKeys), member]));
        ex.InnerException.ShouldBeOfType<NotSupportedException>();
    }

    [Fact]
    public void Include_throws_for_value_type_navigation()
    {
        var (table, _) = CreateParentTable();

        var ex = Should.Throw<NotSupportedException>(() =>
            table
                .Include(p => p.Level)
                .ToArray());

        ex.Message.ShouldContain("reference-type");
    }

    [Fact]
    public void Include_throws_for_non_member_expression()
    {
        var (children, _, _) = CreateParentChildTables();

        var ex = Should.Throw<NotSupportedException>(() =>
            children
                .Include(c => c.ParentId == 0 ? c.Parent : null)
                .ToArray());

        ex.Message.ShouldContain("member access");
    }

    [Fact]
    public void Include_throws_for_nested_member_access()
    {
        var (children, _, _) = CreateParentChildTables();

        var ex = Should.Throw<NotSupportedException>(() =>
            children
                .Include(c => c.Parent!.Name)
                .ToArray());

        ex.Message.ShouldContain("navigation member access chains");
    }

    [Fact]
    public void Include_throws_when_navigation_member_is_not_writable()
    {
        var (table, _) = CreateChildWithReadOnlyParentTable();

        var ex = Should.Throw<NotSupportedException>(() =>
            table
                .Include(c => c.Parent)
                .ToArray());

        ex.Message.ShouldContain("must be writable");
    }

    [Fact]
    public void Include_throws_when_navigation_is_not_configured()
    {
        var (table, _) = CreateChildWithUnconfiguredParentTable();

        var ex = Should.Throw<NotSupportedException>(() =>
            table
                .Include(c => c.Parent)
                .ToArray());

        ex.Message.ShouldContain("not configured");
    }

    [Fact]
    public void Reverse_is_not_supported()
    {
        var (table, _) = CreateParentTable();

        var ex = Should.Throw<NotSupportedException>(() =>
            table
                .Reverse()
                .ToArray());

        ex.Message.ShouldContain("Unsupported Queryable operator");
        ex.Message.ShouldContain("Reverse");
    }

    [Fact]
    public void Include_reference_navigation_materializes_related_entities_and_handles_missing_keys()
    {
        var (children, _, _) = CreateParentChildTables();

        var result = children
            .Include(c => c.Parent)
            .ToArray();

        result.Length.ShouldBe(3);

        result[0].ParentId.ShouldBe(1);
        result[0].Parent.ShouldNotBeNull();
        result[0].Parent!.Id.ShouldBe(1);

        result[1].ParentId.ShouldBe(2);
        result[1].Parent.ShouldNotBeNull();
        result[1].Parent!.Id.ShouldBe(2);

        result[2].ParentId.ShouldBe(0);
        result[2].Parent.ShouldBeNull();
    }

    [Fact]
    public void Include_collection_navigation_dependent_foreign_key_materializes_arrays()
    {
        var (parents, _, _) = CreateParentChildArrayNavigationTables();

        var result = parents
            .Include(p => p.Children)
            .ToArray();

        result.Length.ShouldBe(2);

        var p1 = result.Single(p => p.Id == 1);
        p1.Children.Length.ShouldBe(2);
        p1.Children.Select(c => c.Id).OrderBy(x => x).ShouldBe([10, 11]);

        var p2 = result.Single(p => p.Id == 2);
        p2.Children.Length.ShouldBe(1);
        p2.Children[0].Id.ShouldBe(12);
    }

    [Fact]
    public void Include_collection_navigation_foreign_key_array_materializes_aligned_arrays()
    {
        var (parents, _, _) = CreateParentWithChildIdsArrayNavigationTables();

        var result = parents
            .Include(p => p.Children)
            .ToArray();

        result.Length.ShouldBe(1);
        var parent = result[0];

        parent.ChildIds.ShouldBe([10, 999, 0, 11]);
        parent.Children.Length.ShouldBe(4);
        parent.Children[0].Id.ShouldBe(10);
        parent.Children[1].ShouldBeNull();
        parent.Children[2].ShouldBeNull();
        parent.Children[3].Id.ShouldBe(11);
    }

    [Fact]
    public void Select_scalar_take_can_use_pruned_projection()
    {
        var (table, _) = CreateParentTable();

        var result = table
            .Where(p => p.Level > 0)
            .Select(p => p.Id)
            .Take(2)
            .ToArray();

        result.ShouldBe([1, 2]);
    }

    [Fact]
    public void Select_navigation_can_use_navigation_projection_prune_and_semi_join_predicate()
    {
        var (children, _, _) = CreateParentChildTables();

        var result = children
            .Include(c => c.Parent)
            .Where(c => c.Parent != null && c.Parent.Level > 3)
            .Select(c => c.Parent!.Name)
            .ToArray();

        result.ShouldBe(["p2"]);
    }

    [Fact]
    public void Where_predicate_that_throws_argument_null_is_treated_as_false()
    {
        var (table, _) = CreateParentTable();

        var result = table
            .Where(p => AlwaysThrowsArgNull(p))
            .ToArray();

        result.ShouldBeEmpty();
    }

    [Fact]
    public void Scalar_operators_execute_via_provider()
    {
        var (table, _) = CreateParentTable();

        table.Any().ShouldBeTrue();
        table.Where(p => p.Level > 999).Any().ShouldBeFalse();
        table.Count().ShouldBe(3);
        table.All(p => p.Id > 0).ShouldBeTrue();
    }

    private static (Db2Table<Parent> Table, Db2QueryProvider<Parent, RowHandle> Provider) CreateParentTable()
    {
        var builder = new Db2ModelBuilder();
        builder.Entity<Parent>().HasKey(x => x.Id);
        var model = builder.Build(SchemaResolver);

        var parentsFile = InMemoryDb2File.Create(
            tableName: nameof(Parent),
            flags: Db2Flags.None,
            denseStringTableBytes: ReadOnlyMemory<byte>.Empty,
            rows:
            [
                new InMemoryDb2File.Row(1, [1, 1, "p1"]),
                new InMemoryDb2File.Row(2, [2, 5, "p2"]),
                new InMemoryDb2File.Row(3, [3, 0, "p3"]),
            ]);

        (IDb2File<RowHandle> File, Db2TableSchema Schema) TableResolver(string tableName)
            => tableName switch
            {
                nameof(Parent) => (parentsFile, SchemaResolver(nameof(Parent))),
                _ => throw new InvalidOperationException($"Unknown table: {tableName}"),
            };

        var provider = new Db2QueryProvider<Parent, RowHandle>(parentsFile, model, TableResolver, new ReflectionDb2EntityFactory());

        var entityType = model.GetEntityType(typeof(Parent));
        var schema = SchemaResolver(nameof(Parent));
        var table = new Db2Table<Parent, RowHandle>(nameof(Parent), schema, entityType, provider, parentsFile);
        return (table, provider);
    }

    private static (Db2Table<ChildWithReadOnlyParent> Table, Db2QueryProvider<ChildWithReadOnlyParent, RowHandle> Provider) CreateChildWithReadOnlyParentTable()
    {
        var builder = new Db2ModelBuilder();
        builder.Entity<ChildWithReadOnlyParent>().HasKey(x => x.Id);

        var model = builder.Build(SchemaResolver);

        var file = InMemoryDb2File.Create(
            tableName: nameof(ChildWithReadOnlyParent),
            flags: Db2Flags.None,
            denseStringTableBytes: ReadOnlyMemory<byte>.Empty,
            rows:
            [
                new InMemoryDb2File.Row(10, [10, 1]),
            ]);

        (IDb2File<RowHandle> File, Db2TableSchema Schema) TableResolver(string tableName)
            => tableName switch
            {
                nameof(ChildWithReadOnlyParent) => (file, SchemaResolver(nameof(ChildWithReadOnlyParent))),
                _ => throw new InvalidOperationException($"Unknown table: {tableName}"),
            };

        var provider = new Db2QueryProvider<ChildWithReadOnlyParent, RowHandle>(file, model, TableResolver, new ReflectionDb2EntityFactory());

        var entityType = model.GetEntityType(typeof(ChildWithReadOnlyParent));
        var schema = SchemaResolver(nameof(ChildWithReadOnlyParent));
        var table = new Db2Table<ChildWithReadOnlyParent, RowHandle>(nameof(ChildWithReadOnlyParent), schema, entityType, provider, file);
        return (table, provider);
    }

    private static (Db2Table<ChildWithUnconfiguredParent> Table, Db2QueryProvider<ChildWithUnconfiguredParent, RowHandle> Provider) CreateChildWithUnconfiguredParentTable()
    {
        var builder = new Db2ModelBuilder();
        builder.Entity<ChildWithUnconfiguredParent>().HasKey(x => x.Id);

        var model = builder.Build(SchemaResolver);

        var file = InMemoryDb2File.Create(
            tableName: nameof(ChildWithUnconfiguredParent),
            flags: Db2Flags.None,
            denseStringTableBytes: ReadOnlyMemory<byte>.Empty,
            rows:
            [
                new InMemoryDb2File.Row(10, [10, 1]),
            ]);

        (IDb2File<RowHandle> File, Db2TableSchema Schema) TableResolver(string tableName)
            => tableName switch
            {
                nameof(ChildWithUnconfiguredParent) => (file, SchemaResolver(nameof(ChildWithUnconfiguredParent))),
                _ => throw new InvalidOperationException($"Unknown table: {tableName}"),
            };

        var provider = new Db2QueryProvider<ChildWithUnconfiguredParent, RowHandle>(file, model, TableResolver, new ReflectionDb2EntityFactory());

        var entityType = model.GetEntityType(typeof(ChildWithUnconfiguredParent));
        var schema = SchemaResolver(nameof(ChildWithUnconfiguredParent));
        var table = new Db2Table<ChildWithUnconfiguredParent, RowHandle>(nameof(ChildWithUnconfiguredParent), schema, entityType, provider, file);
        return (table, provider);
    }

    private static (Db2Table<Child> Children, Db2Table<Parent> Parents, Db2Model Model) CreateParentChildTables()
    {
        var builder = new Db2ModelBuilder();

        builder.Entity<Child>()
            .HasOne(x => x.Parent)
            .WithForeignKey(x => x.ParentId);

        builder.Entity<Parent>()
            .HasMany(x => x.Children)
            .WithForeignKey(x => x.ParentId);

        builder.Entity<Parent>().HasKey(x => x.Id);
        builder.Entity<Child>().HasKey(x => x.Id);

        var model = builder.Build(SchemaResolver);

        var parentsFile = InMemoryDb2File.Create(
            tableName: nameof(Parent),
            flags: Db2Flags.None,
            denseStringTableBytes: ReadOnlyMemory<byte>.Empty,
            rows:
            [
                new InMemoryDb2File.Row(1, [1, 1, "p1"]),
                new InMemoryDb2File.Row(2, [2, 5, "p2"]),
            ]);

        var childrenFile = InMemoryDb2File.Create(
            tableName: nameof(Child),
            flags: Db2Flags.None,
            denseStringTableBytes: ReadOnlyMemory<byte>.Empty,
            rows:
            [
                new InMemoryDb2File.Row(10, [10, 1, "c1"]),
                new InMemoryDb2File.Row(11, [11, 2, "c2"]),
                new InMemoryDb2File.Row(12, [12, 0, "c0"]),
            ]);

        (IDb2File<RowHandle> File, Db2TableSchema Schema) TableResolver(string tableName)
            => tableName switch
            {
                nameof(Parent) => (parentsFile, SchemaResolver(nameof(Parent))),
                nameof(Child) => (childrenFile, SchemaResolver(nameof(Child))),
                _ => throw new InvalidOperationException($"Unknown table: {tableName}"),
            };

        var parentsProvider = new Db2QueryProvider<Parent, RowHandle>(parentsFile, model, TableResolver, new ReflectionDb2EntityFactory());
        var childrenProvider = new Db2QueryProvider<Child, RowHandle>(childrenFile, model, TableResolver, new ReflectionDb2EntityFactory());

        var parentEntityType = model.GetEntityType(typeof(Parent));
        var childEntityType = model.GetEntityType(typeof(Child));

        var parents = new Db2Table<Parent, RowHandle>(nameof(Parent), SchemaResolver(nameof(Parent)), parentEntityType, parentsProvider, parentsFile);
        var children = new Db2Table<Child, RowHandle>(nameof(Child), SchemaResolver(nameof(Child)), childEntityType, childrenProvider, childrenFile);

        return (children, parents, model);
    }

    private static (Db2Table<ParentWithChildIds> Parent, Db2Table<Child> Children, Db2Model Model) CreateParentWithChildIdsTables()
    {
        var builder = new Db2ModelBuilder();

        builder.Entity<ParentWithChildIds>()
            .HasMany(x => x.Children)
            .WithForeignKeyArray(x => x.ChildIds);

        builder.Entity<ParentWithChildIds>().HasKey(x => x.Id);
        builder.Entity<Child>().HasKey(x => x.Id);

        var model = builder.Build(SchemaResolver);

        var parentFile = InMemoryDb2File.Create(
            tableName: nameof(ParentWithChildIds),
            flags: Db2Flags.None,
            denseStringTableBytes: ReadOnlyMemory<byte>.Empty,
            rows:
            [
                new InMemoryDb2File.Row(1000, [1000, new[] { 10, 999, 11 }]),
            ]);

        var childFile = InMemoryDb2File.Create(
            tableName: nameof(Child),
            flags: Db2Flags.None,
            denseStringTableBytes: ReadOnlyMemory<byte>.Empty,
            rows:
            [
                new InMemoryDb2File.Row(10, [10, 0, "c1"]),
                new InMemoryDb2File.Row(11, [11, 0, "c2"]),
            ]);

        (IDb2File<RowHandle> File, Db2TableSchema Schema) TableResolver(string tableName)
            => tableName switch
            {
                nameof(ParentWithChildIds) => (parentFile, SchemaResolver(nameof(ParentWithChildIds))),
                nameof(Child) => (childFile, SchemaResolver(nameof(Child))),
                _ => throw new InvalidOperationException($"Unknown table: {tableName}"),
            };

        var parentProvider = new Db2QueryProvider<ParentWithChildIds, RowHandle>(parentFile, model, TableResolver, new ReflectionDb2EntityFactory());
        var childProvider = new Db2QueryProvider<Child, RowHandle>(childFile, model, TableResolver, new ReflectionDb2EntityFactory());

        var parentEntityType = model.GetEntityType(typeof(ParentWithChildIds));
        var childEntityType = model.GetEntityType(typeof(Child));

        var parentTable = new Db2Table<ParentWithChildIds, RowHandle>(nameof(ParentWithChildIds), SchemaResolver(nameof(ParentWithChildIds)), parentEntityType, parentProvider, parentFile);
        var childTable = new Db2Table<Child, RowHandle>(nameof(Child), SchemaResolver(nameof(Child)), childEntityType, childProvider, childFile);

        return (parentTable, childTable, model);
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

            nameof(ParentWithChildIds) => new Db2TableSchema(
                tableName: nameof(ParentWithChildIds),
                layoutHash: 0,
                physicalColumnCount: 2,
                fields:
                [
                    new Db2FieldSchema("ID", Db2ValueType.Int64, ColumnStartIndex: 0, ElementCount: 1, IsVerified: true, IsVirtual: false, IsId: true, IsRelation: false, ReferencedTableName: null),
                    new Db2FieldSchema(nameof(ParentWithChildIds.ChildIds), Db2ValueType.Int64, ColumnStartIndex: 1, ElementCount: 3, IsVerified: true, IsVirtual: false, IsId: false, IsRelation: false, ReferencedTableName: nameof(Child)),
                ]),

            nameof(ParentWithChildrenArray) => new Db2TableSchema(
                tableName: nameof(ParentWithChildrenArray),
                layoutHash: 0,
                physicalColumnCount: 1,
                fields:
                [
                    new Db2FieldSchema("ID", Db2ValueType.Int64, ColumnStartIndex: 0, ElementCount: 1, IsVerified: true, IsVirtual: false, IsId: true, IsRelation: false, ReferencedTableName: null),
                ]),

            nameof(ChildForArrayNav) => new Db2TableSchema(
                tableName: nameof(ChildForArrayNav),
                layoutHash: 0,
                physicalColumnCount: 2,
                fields:
                [
                    new Db2FieldSchema("ID", Db2ValueType.Int64, ColumnStartIndex: 0, ElementCount: 1, IsVerified: true, IsVirtual: false, IsId: true, IsRelation: false, ReferencedTableName: null),
                    new Db2FieldSchema(nameof(ChildForArrayNav.ParentId), Db2ValueType.Int64, ColumnStartIndex: 1, ElementCount: 1, IsVerified: true, IsVirtual: false, IsId: false, IsRelation: false, ReferencedTableName: nameof(ParentWithChildrenArray)),
                ]),

            nameof(ParentWithChildIdsArray) => new Db2TableSchema(
                tableName: nameof(ParentWithChildIdsArray),
                layoutHash: 0,
                physicalColumnCount: 2,
                fields:
                [
                    new Db2FieldSchema("ID", Db2ValueType.Int64, ColumnStartIndex: 0, ElementCount: 1, IsVerified: true, IsVirtual: false, IsId: true, IsRelation: false, ReferencedTableName: null),
                    new Db2FieldSchema(nameof(ParentWithChildIdsArray.ChildIds), Db2ValueType.Int64, ColumnStartIndex: 1, ElementCount: 4, IsVerified: true, IsVirtual: false, IsId: false, IsRelation: false, ReferencedTableName: nameof(ChildForArrayNav)),
                ]),

            nameof(ChildWithReadOnlyParent) => new Db2TableSchema(
                tableName: nameof(ChildWithReadOnlyParent),
                layoutHash: 0,
                physicalColumnCount: 2,
                fields:
                [
                    new Db2FieldSchema("ID", Db2ValueType.Int64, ColumnStartIndex: 0, ElementCount: 1, IsVerified: true, IsVirtual: false, IsId: true, IsRelation: false, ReferencedTableName: null),
                    new Db2FieldSchema(nameof(ChildWithReadOnlyParent.ParentId), Db2ValueType.Int64, ColumnStartIndex: 1, ElementCount: 1, IsVerified: true, IsVirtual: false, IsId: false, IsRelation: false, ReferencedTableName: null),
                ]),

            nameof(ChildWithUnconfiguredParent) => new Db2TableSchema(
                tableName: nameof(ChildWithUnconfiguredParent),
                layoutHash: 0,
                physicalColumnCount: 2,
                fields:
                [
                    new Db2FieldSchema("ID", Db2ValueType.Int64, ColumnStartIndex: 0, ElementCount: 1, IsVerified: true, IsVirtual: false, IsId: true, IsRelation: false, ReferencedTableName: null),
                    new Db2FieldSchema(nameof(ChildWithUnconfiguredParent.ParentId), Db2ValueType.Int64, ColumnStartIndex: 1, ElementCount: 1, IsVerified: true, IsVirtual: false, IsId: false, IsRelation: false, ReferencedTableName: null),
                ]),

            _ => throw new InvalidOperationException($"Unknown table: {tableName}"),
        };
    }

    private static (Db2Table<ParentWithChildrenArray> Parents, Db2Table<ChildForArrayNav> Children, Db2Model Model) CreateParentChildArrayNavigationTables()
    {
        var builder = new Db2ModelBuilder();

        builder.Entity<ChildForArrayNav>()
            .HasOne(x => x.Parent)
            .WithForeignKey(x => x.ParentId);

        builder.Entity<ParentWithChildrenArray>()
            .HasMany(x => x.Children)
            .WithForeignKey(x => x.ParentId);

        builder.Entity<ParentWithChildrenArray>().HasKey(x => x.Id);
        builder.Entity<ChildForArrayNav>().HasKey(x => x.Id);

        var model = builder.Build(SchemaResolver);

        var parentsFile = InMemoryDb2File.Create(
            tableName: nameof(ParentWithChildrenArray),
            flags: Db2Flags.None,
            denseStringTableBytes: ReadOnlyMemory<byte>.Empty,
            rows:
            [
                new InMemoryDb2File.Row(1, [1]),
                new InMemoryDb2File.Row(2, [2]),
            ]);

        var childrenFile = InMemoryDb2File.Create(
            tableName: nameof(ChildForArrayNav),
            flags: Db2Flags.None,
            denseStringTableBytes: ReadOnlyMemory<byte>.Empty,
            rows:
            [
                new InMemoryDb2File.Row(10, [10, 1]),
                new InMemoryDb2File.Row(11, [11, 1]),
                new InMemoryDb2File.Row(12, [12, 2]),
                new InMemoryDb2File.Row(13, [13, 0]),
            ]);

        (IDb2File<RowHandle> File, Db2TableSchema Schema) TableResolver(string tableName)
            => tableName switch
            {
                nameof(ParentWithChildrenArray) => (parentsFile, SchemaResolver(nameof(ParentWithChildrenArray))),
                nameof(ChildForArrayNav) => (childrenFile, SchemaResolver(nameof(ChildForArrayNav))),
                _ => throw new InvalidOperationException($"Unknown table: {tableName}"),
            };

        var parentsProvider = new Db2QueryProvider<ParentWithChildrenArray, RowHandle>(parentsFile, model, TableResolver, new ReflectionDb2EntityFactory());
        var childrenProvider = new Db2QueryProvider<ChildForArrayNav, RowHandle>(childrenFile, model, TableResolver, new ReflectionDb2EntityFactory());

        var parentEntityType = model.GetEntityType(typeof(ParentWithChildrenArray));
        var childEntityType = model.GetEntityType(typeof(ChildForArrayNav));

        var parents = new Db2Table<ParentWithChildrenArray, RowHandle>(nameof(ParentWithChildrenArray), SchemaResolver(nameof(ParentWithChildrenArray)), parentEntityType, parentsProvider, parentsFile);
        var children = new Db2Table<ChildForArrayNav, RowHandle>(nameof(ChildForArrayNav), SchemaResolver(nameof(ChildForArrayNav)), childEntityType, childrenProvider, childrenFile);

        return (parents, children, model);
    }

    private static (Db2Table<ParentWithChildIdsArray> Parents, Db2Table<ChildForArrayNav> Children, Db2Model Model) CreateParentWithChildIdsArrayNavigationTables()
    {
        var builder = new Db2ModelBuilder();

        builder.Entity<ParentWithChildIdsArray>()
            .HasMany(x => x.Children)
            .WithForeignKeyArray(x => x.ChildIds);

        builder.Entity<ParentWithChildIdsArray>().HasKey(x => x.Id);
        builder.Entity<ChildForArrayNav>().HasKey(x => x.Id);

        var model = builder.Build(SchemaResolver);

        var parentFile = InMemoryDb2File.Create(
            tableName: nameof(ParentWithChildIdsArray),
            flags: Db2Flags.None,
            denseStringTableBytes: ReadOnlyMemory<byte>.Empty,
            rows:
            [
                new InMemoryDb2File.Row(1000, [1000, new[] { 10, 999, 0, 11 }]),
            ]);

        var childFile = InMemoryDb2File.Create(
            tableName: nameof(ChildForArrayNav),
            flags: Db2Flags.None,
            denseStringTableBytes: ReadOnlyMemory<byte>.Empty,
            rows:
            [
                new InMemoryDb2File.Row(10, [10, 0]),
                new InMemoryDb2File.Row(11, [11, 0]),
            ]);

        (IDb2File<RowHandle> File, Db2TableSchema Schema) TableResolver(string tableName)
            => tableName switch
            {
                nameof(ParentWithChildIdsArray) => (parentFile, SchemaResolver(nameof(ParentWithChildIdsArray))),
                nameof(ChildForArrayNav) => (childFile, SchemaResolver(nameof(ChildForArrayNav))),
                _ => throw new InvalidOperationException($"Unknown table: {tableName}"),
            };

        var parentProvider = new Db2QueryProvider<ParentWithChildIdsArray, RowHandle>(parentFile, model, TableResolver, new ReflectionDb2EntityFactory());
        var childProvider = new Db2QueryProvider<ChildForArrayNav, RowHandle>(childFile, model, TableResolver, new ReflectionDb2EntityFactory());

        var parentEntityType = model.GetEntityType(typeof(ParentWithChildIdsArray));
        var childEntityType = model.GetEntityType(typeof(ChildForArrayNav));

        var parents = new Db2Table<ParentWithChildIdsArray, RowHandle>(nameof(ParentWithChildIdsArray), SchemaResolver(nameof(ParentWithChildIdsArray)), parentEntityType, parentProvider, parentFile);
        var children = new Db2Table<ChildForArrayNav, RowHandle>(nameof(ChildForArrayNav), SchemaResolver(nameof(ChildForArrayNav)), childEntityType, childProvider, childFile);

        return (parents, children, model);
    }

    private static bool AlwaysThrowsArgNull(Parent _)
        => throw new ArgumentNullException("boom");

    private static bool AlwaysThrowsNullRef(Parent _)
        => throw new NullReferenceException("boom");

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

    private sealed class ChildWithReadOnlyParent
    {
        public int Id { get; set; }

        public int ParentId { get; set; }

        public Parent? Parent { get; }
    }

    private sealed class ChildWithUnconfiguredParent
    {
        public int Id { get; set; }

        public int ParentId { get; set; }

        public Parent? Parent { get; set; }
    }

    private sealed class ParentWithChildIds
    {
        public int Id { get; set; }

        public int[] ChildIds { get; set; } = [];

        public ICollection<Child> Children { get; set; } = [];
    }

    private sealed class ParentWithChildrenArray
    {
        public int Id { get; set; }

        public ChildForArrayNav[] Children { get; set; } = [];
    }

    private sealed class ParentWithChildIdsArray
    {
        public int Id { get; set; }

        public int[] ChildIds { get; set; } = [];

        public ChildForArrayNav[] Children { get; set; } = [];
    }

    private sealed class ChildForArrayNav
    {
        public int Id { get; set; }

        public int ParentId { get; set; }

        public ParentWithChildrenArray? Parent { get; set; }
    }

    private sealed class InMemoryDb2File(string tableName, Db2Flags flags, ReadOnlyMemory<byte> denseStringTableBytes, IReadOnlyDictionary<int, object[]> valuesByRowId)
        : IDb2File<RowHandle>
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

        public static InMemoryDb2File Create(string tableName, Db2Flags flags, ReadOnlyMemory<byte> denseStringTableBytes, IReadOnlyList<Row> rows)
        {
            return new InMemoryDb2File(
                tableName,
                flags,
                denseStringTableBytes,
                rows.ToDictionary(r => r.Id, r => r.Values));
        }

        public readonly record struct Row(int Id, object[] Values);
    }

    private enum TestKey
    {
        Ten = 10,
        Eleven = 11,
    }

    private sealed class EntityWithNullableEnumKeys
    {
        public TestKey?[] Keys { get; set; } = [];
    }

    private sealed class EntityWithStringKeys
    {
        public string Keys { get; set; } = string.Empty;
    }
}
