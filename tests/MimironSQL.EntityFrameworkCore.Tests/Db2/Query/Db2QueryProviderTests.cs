using System.Linq.Expressions;
using System.Reflection;

using Microsoft.EntityFrameworkCore;

using MimironSQL.Db2;
using MimironSQL.EntityFrameworkCore.Db2.Model;
using MimironSQL.EntityFrameworkCore.Db2.Query;
using MimironSQL.EntityFrameworkCore.Db2.Schema;
using MimironSQL.Formats;

using Shouldly;

namespace MimironSQL.EntityFrameworkCore.Tests;

public sealed class Db2QueryProviderTests
{
    [Fact]
    public void Db2QueryPipeline_recognizes_Include_expression()
    {
        var (children, _, _) = CreateParentChildTables();

        var query = EfInclude(children, c => c.Parent);

        var pipeline = Db2QueryPipeline.Parse(query.Expression);
        var includeCount = pipeline.Operations.OfType<Db2IncludeOperation>().Count();

        includeCount.ShouldBe(1, "Include not recognized. Call chain: " + DescribeCallChain(query.Expression));
    }

    private static string DescribeCallChain(Expression expression)
    {
        var parts = new List<string>();
        var current = expression;

        while (current is MethodCallExpression m)
        {
            parts.Add($"{m.Method.DeclaringType?.FullName ?? "<null>"}.{m.Method.Name} [static={m.Method.IsStatic}] (args={m.Arguments.Count})");
            current = m.Arguments[0];
        }

        return string.Join(" -> ", parts);
    }

    private static IQueryable<TEntity> EfInclude<TEntity, TProperty>(IQueryable<TEntity> source, Expression<Func<TEntity, TProperty>> navigation)
    {
        ArgumentNullException.ThrowIfNull(source);
        ArgumentNullException.ThrowIfNull(navigation);

        var include = typeof(EntityFrameworkQueryableExtensions)
            .GetMethods(BindingFlags.Public | BindingFlags.Static)
            .Single(m => m is { Name: "Include", IsGenericMethodDefinition: true } && m.GetGenericArguments().Length == 2 && m.GetParameters().Length == 2);

        var call = Expression.Call(
            include.MakeGenericMethod(typeof(TEntity), typeof(TProperty)),
            source.Expression,
            Expression.Quote(navigation));

        return source.Provider.CreateQuery<TEntity>(call);
    }

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

        var query = provider.CreateQuery(whereCall);

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

        var obj = provider.Execute(query.Expression);
        obj.ShouldNotBeNull();

        var items = (IEnumerable<Parent>)obj!;
        items.Select(x => x.Id).OrderBy(x => x).ToArray().ShouldBe([1, 2]);
    }

    [Fact]
    public void Execute_non_generic_throws_for_scalar_expressions_and_string_is_not_treated_as_enumerable()
    {
        var (_, provider) = CreateParentTable();

        Should.Throw<NotSupportedException>(() => provider.Execute(Expression.Constant(123)));
        Should.Throw<NotSupportedException>(() => provider.Execute(Expression.Constant("hello")));
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

        var result = EfInclude(children, c => c.Parent)
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

        var result = EfInclude(parents, p => p.Children)
            .ToArray()
            .OrderBy(p => p.Id)
            .ToArray();

        result.Length.ShouldBe(2);

        var p1 = result[0];
        p1.Id.ShouldBe(1);
        p1.Children.Select(c => c.Id).OrderBy(x => x).ToArray().ShouldBe([10]);
        Should.Throw<NotSupportedException>(() => p1.Children.Add(new Child { Id = 999 }));

        var p2 = result[1];
        p2.Id.ShouldBe(2);
        p2.Children.Select(c => c.Id).OrderBy(x => x).ToArray().ShouldBe([11]);
        Should.Throw<NotSupportedException>(() => p2.Children.Add(new Child { Id = 999 }));
    }

    [Fact]
    public void Include_collection_navigation_foreign_key_array_preserves_id_order()
    {
        var (parentWithIds, _, _) = CreateParentWithChildIdsTables();

        var result = EfInclude(parentWithIds, p => p.Children)
            .ToArray();

        result.Length.ShouldBe(1);

        // Missing IDs are filtered out; the resulting collection is compact.
        result[0].Children.Select(c => c.Id).ToArray().ShouldBe([10, 11]);
    }

    [Fact]
    public void Navigation_projection_pruning_can_apply_semi_join_predicates_and_project_related_values()
    {
        var (children, _, _) = CreateParentChildTables();

        var results = EfInclude(children, c => c.Parent)
            .Where(c => c.Parent!.Level > 3)
            .Select(c => c.Parent!.Name)
            .ToArray();

        results.ShouldBe(["p2"]);
    }

    [Fact]
    public void Navigation_access_with_multiple_Selects_includes_navigation_then_projects()
    {
        var (children, _, _) = CreateParentChildTables();

        var results = EfInclude(children, c => c.Parent)
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
            EfInclude(children.Select(c => c.Parent!), p => p.Children).ToArray());

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
            EfInclude(table, p => p.Level).ToArray());

        ex.Message.ShouldContain("reference-type");
    }

    [Fact]
    public void Include_throws_for_non_member_expression()
    {
        var (children, _, _) = CreateParentChildTables();

        var ex = Should.Throw<NotSupportedException>(() =>
            EfInclude(children, c => c.ParentId == 0 ? c.Parent : null).ToArray());

        ex.Message.ShouldContain("member access");
    }

    [Fact]
    public void Include_throws_for_nested_member_access()
    {
        var (children, _, _) = CreateParentChildTables();

        var ex = Should.Throw<NotSupportedException>(() =>
            EfInclude(children, c => c.Parent!.Name).ToArray());

        ex.Message.ShouldContain("navigation member access chains");
    }

    [Fact]
    public void Include_throws_when_navigation_member_is_not_writable()
    {
        var (table, _) = CreateChildWithReadOnlyParentTable();

        var ex = Should.Throw<NotSupportedException>(() =>
            EfInclude(table, c => c.Parent).ToArray());

        ex.Message.ShouldContain("must be writable");
    }

    [Fact]
    public void Include_throws_when_navigation_is_not_configured()
    {
        var (table, _) = CreateChildWithUnconfiguredParentTable();

        var ex = Should.Throw<NotSupportedException>(() =>
            EfInclude(table, c => c.Parent).ToArray());

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

        var result = EfInclude(children, c => c.Parent)
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

        var result = EfInclude(children, c => c.Parent)
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

    private static (IQueryable<Parent> Table, Db2QueryProvider<Parent, RowHandle> Provider) CreateParentTable()
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

        return (new Db2Queryable<Parent>(provider), provider);
    }

    private static (IQueryable<ChildWithReadOnlyParent> Table, Db2QueryProvider<ChildWithReadOnlyParent, RowHandle> Provider) CreateChildWithReadOnlyParentTable()
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

        return (new Db2Queryable<ChildWithReadOnlyParent>(provider), provider);
    }

    private static (IQueryable<ChildWithUnconfiguredParent> Table, Db2QueryProvider<ChildWithUnconfiguredParent, RowHandle> Provider) CreateChildWithUnconfiguredParentTable()
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

        return (new Db2Queryable<ChildWithUnconfiguredParent>(provider), provider);
    }

    private static (IQueryable<Child> Children, IQueryable<Parent> Parents, Db2Model Model) CreateParentChildTables()
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

        return (new Db2Queryable<Child>(childrenProvider), new Db2Queryable<Parent>(parentsProvider), model);
    }

    private static (IQueryable<ParentWithChildIds> Parent, IQueryable<Child> Children, Db2Model Model) CreateParentWithChildIdsTables()
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

        return (new Db2Queryable<ParentWithChildIds>(parentProvider), new Db2Queryable<Child>(childProvider), model);
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
