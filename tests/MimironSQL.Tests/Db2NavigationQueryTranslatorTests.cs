using System.Reflection;

using MimironSQL.Db2;
using MimironSQL.Db2.Model;
using MimironSQL.Db2.Query;
using MimironSQL.Db2.Schema;

using Shouldly;

namespace MimironSQL.Tests;

public sealed class Db2NavigationQueryTranslatorTests
{
    [Fact]
    public void TryTranslateScalarPredicate_compiles_int_constant_and_captured_values()
    {
        var model = CreateModel();

        var threshold = 3;
        Db2NavigationQueryTranslator
            .TryTranslateScalarPredicate<Child>(model, x => x.Parent!.Level > threshold, out var plan)
            .ShouldBeTrue();

        plan.ShouldNotBeNull();
        plan.TargetScalarMember.Name.ShouldBe(nameof(Parent.Level));
        plan.ComparisonKind.ShouldBe(Db2ScalarComparisonKind.GreaterThan);
    }

    [Fact]
    public void TryTranslateScalarPredicate_handles_enum_underlying_type()
    {
        var model = CreateModel();

        Db2NavigationQueryTranslator
            .TryTranslateScalarPredicate<Child>(model, x => x.Parent!.Kind == ParentKind.Beta, out var plan)
            .ShouldBeTrue();

        plan.ShouldNotBeNull();
        plan.TargetScalarMember.Name.ShouldBe(nameof(Parent.Kind));
        plan.ComparisonKind.ShouldBe(Db2ScalarComparisonKind.Equal);
    }

    [Fact]
    public void TryTranslateStringPredicate_supports_contains()
    {
        var model = CreateModel();

        Db2NavigationQueryTranslator
            .TryTranslateStringPredicate<Child>(model, x => x.Parent!.Name.Contains("abc"), out var plan)
            .ShouldBeTrue();

        plan.MatchKind.ShouldBe(Db2NavigationStringMatchKind.Contains);
        plan.Needle.ShouldBe("abc");
        plan.TargetStringMember.Name.ShouldBe(nameof(Parent.Name));
    }

    [Fact]
    public void TryTranslateNullCheck_supports_not_null_and_null()
    {
        var model = CreateModel();

        Db2NavigationQueryTranslator
            .TryTranslateNullCheck<Child>(model, x => x.Parent != null, out var notNull)
            .ShouldBeTrue();

        notNull.IsNotNull.ShouldBeTrue();

        Db2NavigationQueryTranslator
            .TryTranslateNullCheck<Child>(model, x => x.Parent == null, out var isNull)
            .ShouldBeTrue();

        isNull.IsNotNull.ShouldBeFalse();
    }

    [Fact]
    public void GetNavigationAccesses_finds_one_hop_member_accesses()
    {
        var model = CreateModel();

        var accesses = Db2NavigationQueryTranslator.GetNavigationAccesses<Child>(
            model,
            (Child x) => new { x.Id, x.Parent!.Name, x.Parent.Level });

        accesses.Count.ShouldBe(2);
        accesses.Select(a => a.TargetMember.Name).OrderBy(x => x, StringComparer.Ordinal)
            .ShouldBe([nameof(Parent.Level), nameof(Parent.Name)]);
    }

    [Fact]
    public void TryTranslateCollectionAnyPredicate_supports_Any_with_and_without_dependent_predicate()
    {
        var builder = new Db2ModelBuilder();

        builder.Entity<Parent>()
            .HasMany(x => x.Children)
            .WithForeignKey(x => x.ParentId);

        builder.Entity<Parent>().HasKey(x => x.Id);
        builder.Entity<Child>().HasKey(x => x.Id);

        var model = builder.Build(SchemaResolver);

        Db2NavigationQueryTranslator
            .TryTranslateCollectionAnyPredicate<Parent>(model, x => x.Children.Any(), out var any)
            .ShouldBeTrue();

        any.DependentPredicate.ShouldBeNull();

        Db2NavigationQueryTranslator
            .TryTranslateCollectionAnyPredicate<Parent>(model, x => x.Children.Any(c => c.ParentId > 0), out var anyWithPredicate)
            .ShouldBeTrue();

        anyWithPredicate.DependentPredicate.ShouldNotBeNull();
        anyWithPredicate.DependentPredicate!.Parameters.Count.ShouldBe(1);
        anyWithPredicate.DependentPredicate.Parameters[0].Type.ShouldBe(typeof(Child));
    }

    private static Db2Model CreateModel()
    {
        var builder = new Db2ModelBuilder();

        builder.Entity<Child>()
            .HasOne(x => x.Parent)
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
                physicalColumnCount: 4,
                fields:
                [
                    new Db2FieldSchema("ID", Db2ValueType.Int64, ColumnStartIndex: 0, ElementCount: 1, IsVerified: true, IsVirtual: false, IsId: true, IsRelation: false, ReferencedTableName: null),
                    new Db2FieldSchema(nameof(Parent.Level), Db2ValueType.Int64, ColumnStartIndex: 1, ElementCount: 1, IsVerified: true, IsVirtual: false, IsId: false, IsRelation: false, ReferencedTableName: null),
                    new Db2FieldSchema(nameof(Parent.Kind), Db2ValueType.Int64, ColumnStartIndex: 2, ElementCount: 1, IsVerified: true, IsVirtual: false, IsId: false, IsRelation: false, ReferencedTableName: null),
                    new Db2FieldSchema(nameof(Parent.Name), Db2ValueType.String, ColumnStartIndex: 3, ElementCount: 1, IsVerified: true, IsVirtual: false, IsId: false, IsRelation: false, ReferencedTableName: null),
                ]),

            nameof(Child) => new Db2TableSchema(
                tableName: nameof(Child),
                layoutHash: 0,
                physicalColumnCount: 2,
                fields:
                [
                    new Db2FieldSchema("ID", Db2ValueType.Int64, ColumnStartIndex: 0, ElementCount: 1, IsVerified: true, IsVirtual: false, IsId: true, IsRelation: false, ReferencedTableName: null),
                    new Db2FieldSchema(nameof(Child.ParentId), Db2ValueType.Int64, ColumnStartIndex: 1, ElementCount: 1, IsVerified: true, IsVirtual: false, IsId: false, IsRelation: false, ReferencedTableName: nameof(Parent)),
                ]),

            _ => throw new InvalidOperationException($"Unknown table: {tableName}"),
        };
    }

    private enum ParentKind
    {
        Alpha = 0,
        Beta = 1,
    }

    private sealed class Parent
    {
        public int Id { get; set; }

        public int Level { get; set; }

        public ParentKind Kind { get; set; }

        public string Name { get; set; } = string.Empty;

        public ICollection<Child> Children { get; set; } = [];
    }

    private sealed class Child
    {
        public int Id { get; set; }

        public int ParentId { get; set; }

        public Parent? Parent { get; set; }
    }
}
