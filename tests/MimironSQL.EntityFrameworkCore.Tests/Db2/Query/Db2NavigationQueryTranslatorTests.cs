using System.Reflection;

using MimironSQL.Db2;
using MimironSQL.Db2.Model;
using MimironSQL.Db2.Query;
using MimironSQL.Db2.Schema;

using Shouldly;

namespace MimironSQL.EntityFrameworkCore.Tests;

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
    public void TryTranslateStringPredicate_supports_equals_with_constant_on_left()
    {
        var model = CreateModel();

        Db2NavigationQueryTranslator
            .TryTranslateStringPredicate<Child>(model, x => "abc" == x.Parent!.Name, out var plan)
            .ShouldBeTrue();

        plan.MatchKind.ShouldBe(Db2NavigationStringMatchKind.Equals);
        plan.Needle.ShouldBe("abc");
    }

    [Fact]
    public void TryTranslateStringPredicate_supports_startswith_and_endswith_with_captured_needle()
    {
        var model = CreateModel();

        var starts = "ab";
        Db2NavigationQueryTranslator
            .TryTranslateStringPredicate<Child>(model, x => x.Parent!.Name.StartsWith(starts), out var startsWith)
            .ShouldBeTrue();
        startsWith.MatchKind.ShouldBe(Db2NavigationStringMatchKind.StartsWith);
        startsWith.Needle.ShouldBe("ab");

        var ends = "yz";
        Db2NavigationQueryTranslator
            .TryTranslateStringPredicate<Child>(model, x => x.Parent!.Name.EndsWith(ends), out var endsWith)
            .ShouldBeTrue();
        endsWith.MatchKind.ShouldBe(Db2NavigationStringMatchKind.EndsWith);
        endsWith.Needle.ShouldBe("yz");
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
    public void GetNavigationAccesses_handles_method_calls_conditionals_and_member_inits()
    {
        var model = CreateModel();

        var accesses = Db2NavigationQueryTranslator.GetNavigationAccesses<Child>(
            model,
            (Child x) => new Holder
            {
                Name = x.Parent!.Name.ToUpperInvariant(),
                Level = x.Parent != null ? x.Parent.Level : 0,
            });

        accesses.Count.ShouldBe(2);
        accesses.Select(a => a.TargetMember.Name).OrderBy(x => x, StringComparer.Ordinal)
            .ShouldBe([nameof(Parent.Level), nameof(Parent.Name)]);
    }

    [Fact]
    public void TryTranslateScalarPredicate_supports_multiple_scalar_types_and_flipped_comparisons()
    {
        var model = CreateModel();

        Db2NavigationQueryTranslator
            .TryTranslateScalarPredicate<Child>(model, x => x.Parent!.Flag != false, out var boolPlan)
            .ShouldBeTrue();
        boolPlan.TargetScalarMember.Name.ShouldBe(nameof(Parent.Flag));
        boolPlan.ComparisonKind.ShouldBe(Db2ScalarComparisonKind.NotEqual);

        Db2NavigationQueryTranslator
            .TryTranslateScalarPredicate<Child>(model, x => x.Parent!.Small > (byte)2, out var bytePlan)
            .ShouldBeTrue();
        bytePlan.TargetScalarMember.Name.ShouldBe(nameof(Parent.Small));
        bytePlan.ComparisonKind.ShouldBe(Db2ScalarComparisonKind.GreaterThan);

        Db2NavigationQueryTranslator
            .TryTranslateScalarPredicate<Child>(model, x => x.Parent!.Big <= 123L, out var longPlan)
            .ShouldBeTrue();
        longPlan.TargetScalarMember.Name.ShouldBe(nameof(Parent.Big));
        longPlan.ComparisonKind.ShouldBe(Db2ScalarComparisonKind.LessThanOrEqual);

        Db2NavigationQueryTranslator
            .TryTranslateScalarPredicate<Child>(model, x => x.Parent!.Ratio >= 1.5f, out var floatPlan)
            .ShouldBeTrue();
        floatPlan.TargetScalarMember.Name.ShouldBe(nameof(Parent.Ratio));
        floatPlan.ComparisonKind.ShouldBe(Db2ScalarComparisonKind.GreaterThanOrEqual);

        Db2NavigationQueryTranslator
            .TryTranslateScalarPredicate<Child>(model, x => x.Parent!.Score > 0.25d, out var doublePlan)
            .ShouldBeTrue();
        doublePlan.TargetScalarMember.Name.ShouldBe(nameof(Parent.Score));
        doublePlan.ComparisonKind.ShouldBe(Db2ScalarComparisonKind.GreaterThan);

        Db2NavigationQueryTranslator
            .TryTranslateScalarPredicate<Child>(model, x => 3 < x.Parent!.Level, out var flipped)
            .ShouldBeTrue();
        flipped.TargetScalarMember.Name.ShouldBe(nameof(Parent.Level));
        flipped.ComparisonKind.ShouldBe(Db2ScalarComparisonKind.GreaterThan);
    }

    [Fact]
    public void TryTranslateScalarPredicate_supports_more_integer_scalar_types_and_throws_for_unsupported_decimal()
    {
        var model = CreateModel();

        Db2NavigationQueryTranslator
            .TryTranslateScalarPredicate<Child>(model, x => x.Parent!.TinySigned != (sbyte)0, out var sbytePlan)
            .ShouldBeTrue();
        sbytePlan.TargetScalarMember.Name.ShouldBe(nameof(Parent.TinySigned));

        Db2NavigationQueryTranslator
            .TryTranslateScalarPredicate<Child>(model, x => x.Parent!.Shorty >= (short)1, out var shortPlan)
            .ShouldBeTrue();
        shortPlan.TargetScalarMember.Name.ShouldBe(nameof(Parent.Shorty));

        Db2NavigationQueryTranslator
            .TryTranslateScalarPredicate<Child>(model, x => x.Parent!.UShorty <= (ushort)5, out var ushortPlan)
            .ShouldBeTrue();
        ushortPlan.TargetScalarMember.Name.ShouldBe(nameof(Parent.UShorty));

        Db2NavigationQueryTranslator
            .TryTranslateScalarPredicate<Child>(model, x => x.Parent!.ULevel == 7u, out var uintPlan)
            .ShouldBeTrue();
        uintPlan.TargetScalarMember.Name.ShouldBe(nameof(Parent.ULevel));

        Db2NavigationQueryTranslator
            .TryTranslateScalarPredicate<Child>(model, x => x.Parent!.UBig > 0ul, out var ulongPlan)
            .ShouldBeTrue();
        ulongPlan.TargetScalarMember.Name.ShouldBe(nameof(Parent.UBig));

        Db2NavigationQueryTranslator
            .TryTranslateScalarPredicate<Child>(model, x => x.Parent!.Money > 0.0m, out _)
            .ShouldBeFalse();
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

    [Fact]
    public void TryTranslateStringPredicate_supports_equals_with_constant_on_right()
    {
        var model = CreateModel();

        Db2NavigationQueryTranslator
            .TryTranslateStringPredicate<Child>(model, x => x.Parent!.Name == "abc", out var plan)
            .ShouldBeTrue();

        plan.MatchKind.ShouldBe(Db2NavigationStringMatchKind.Equals);
        plan.Needle.ShouldBe("abc");
    }

    [Fact]
    public void TryTranslateStringPredicate_returns_false_when_needle_is_null()
    {
        var model = CreateModel();

        string? needle = null;

        Db2NavigationQueryTranslator
            .TryTranslateStringPredicate<Child>(model, x => x.Parent!.Name == needle, out _)
            .ShouldBeFalse();
    }

    [Fact]
    public void TryTranslateStringPredicate_returns_false_when_needle_evaluation_throws()
    {
        var model = CreateModel();

        Db2NavigationQueryTranslator
            .TryTranslateStringPredicate<Child>(model, x => x.Parent!.Name == ThrowingNeedle(), out _)
            .ShouldBeFalse();
    }

    private static string ThrowingNeedle() => throw new InvalidOperationException("boom");

    [Fact]
    public void TryTranslateScalarPredicate_returns_false_when_comparison_value_depends_on_root_parameter()
    {
        var model = CreateModel();

        Db2NavigationQueryTranslator
            .TryTranslateScalarPredicate<Child>(model, x => x.Parent!.Level > x.Id, out _)
            .ShouldBeFalse();
    }

    [Fact]
    public void TryTranslateNullCheck_returns_false_when_no_side_is_null_constant()
    {
        var model = CreateModel();

        Db2NavigationQueryTranslator
            .TryTranslateNullCheck<Child>(model, x => x.Parent == x.Parent, out _)
            .ShouldBeFalse();
    }

    [Fact]
    public void TryTranslateCollectionAnyPredicate_returns_false_for_queryable_any()
    {
        var builder = new Db2ModelBuilder();

        builder.Entity<Parent>().HasKey(x => x.Id);
        builder.Entity<Child>().HasKey(x => x.Id);

        var model = builder.Build(SchemaResolver);

        Db2NavigationQueryTranslator
            .TryTranslateCollectionAnyPredicate<Parent>(model, x => x.Children.AsQueryable().Any(), out _)
            .ShouldBeFalse();
    }

    [Fact]
    public void GetNavigationAccesses_throws_when_reference_navigation_is_not_configured()
    {
        var builder = new Db2ModelBuilder();

        builder.Entity<Parent>().HasKey(x => x.Id);
        builder.Entity<Child>().HasKey(x => x.Id);

        var model = builder.Build(SchemaResolver);

        Should.Throw<NotSupportedException>(() =>
        {
            _ = Db2NavigationQueryTranslator.GetNavigationAccesses<Child>(
                model,
                (Child x) => new { x.Parent!.Name });
        }).Message.ShouldContain("is not configured");
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
                physicalColumnCount: 15,
                fields:
                [
                    new Db2FieldSchema("ID", Db2ValueType.Int64, ColumnStartIndex: 0, ElementCount: 1, IsVerified: true, IsVirtual: false, IsId: true, IsRelation: false, ReferencedTableName: null),
                    new Db2FieldSchema(nameof(Parent.Level), Db2ValueType.Int64, ColumnStartIndex: 1, ElementCount: 1, IsVerified: true, IsVirtual: false, IsId: false, IsRelation: false, ReferencedTableName: null),
                    new Db2FieldSchema(nameof(Parent.Kind), Db2ValueType.Int64, ColumnStartIndex: 2, ElementCount: 1, IsVerified: true, IsVirtual: false, IsId: false, IsRelation: false, ReferencedTableName: null),
                    new Db2FieldSchema(nameof(Parent.Name), Db2ValueType.String, ColumnStartIndex: 3, ElementCount: 1, IsVerified: true, IsVirtual: false, IsId: false, IsRelation: false, ReferencedTableName: null),
                    new Db2FieldSchema(nameof(Parent.Flag), Db2ValueType.Int64, ColumnStartIndex: 4, ElementCount: 1, IsVerified: true, IsVirtual: false, IsId: false, IsRelation: false, ReferencedTableName: null),
                    new Db2FieldSchema(nameof(Parent.Small), Db2ValueType.Int64, ColumnStartIndex: 5, ElementCount: 1, IsVerified: true, IsVirtual: false, IsId: false, IsRelation: false, ReferencedTableName: null),
                    new Db2FieldSchema(nameof(Parent.Big), Db2ValueType.Int64, ColumnStartIndex: 6, ElementCount: 1, IsVerified: true, IsVirtual: false, IsId: false, IsRelation: false, ReferencedTableName: null),
                    new Db2FieldSchema(nameof(Parent.Ratio), Db2ValueType.Single, ColumnStartIndex: 7, ElementCount: 1, IsVerified: true, IsVirtual: false, IsId: false, IsRelation: false, ReferencedTableName: null),
                    new Db2FieldSchema(nameof(Parent.Score), Db2ValueType.Int64, ColumnStartIndex: 8, ElementCount: 1, IsVerified: true, IsVirtual: false, IsId: false, IsRelation: false, ReferencedTableName: null),
                    new Db2FieldSchema(nameof(Parent.TinySigned), Db2ValueType.Int64, ColumnStartIndex: 9, ElementCount: 1, IsVerified: true, IsVirtual: false, IsId: false, IsRelation: false, ReferencedTableName: null),
                    new Db2FieldSchema(nameof(Parent.Shorty), Db2ValueType.Int64, ColumnStartIndex: 10, ElementCount: 1, IsVerified: true, IsVirtual: false, IsId: false, IsRelation: false, ReferencedTableName: null),
                    new Db2FieldSchema(nameof(Parent.UShorty), Db2ValueType.Int64, ColumnStartIndex: 11, ElementCount: 1, IsVerified: true, IsVirtual: false, IsId: false, IsRelation: false, ReferencedTableName: null),
                    new Db2FieldSchema(nameof(Parent.ULevel), Db2ValueType.Int64, ColumnStartIndex: 12, ElementCount: 1, IsVerified: true, IsVirtual: false, IsId: false, IsRelation: false, ReferencedTableName: null),
                    new Db2FieldSchema(nameof(Parent.UBig), Db2ValueType.Int64, ColumnStartIndex: 13, ElementCount: 1, IsVerified: true, IsVirtual: false, IsId: false, IsRelation: false, ReferencedTableName: null),
                    new Db2FieldSchema(nameof(Parent.Money), Db2ValueType.Int64, ColumnStartIndex: 14, ElementCount: 1, IsVerified: true, IsVirtual: false, IsId: false, IsRelation: false, ReferencedTableName: null),
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

        public bool Flag { get; set; }

        public byte Small { get; set; }

        public long Big { get; set; }

        public float Ratio { get; set; }

        public double Score { get; set; }

        public sbyte TinySigned { get; set; }

        public short Shorty { get; set; }

        public ushort UShorty { get; set; }

        public uint ULevel { get; set; }

        public ulong UBig { get; set; }

        public decimal Money { get; set; }

        public ICollection<Child> Children { get; set; } = [];
    }

    private sealed class Holder
    {
        public string Name { get; set; } = string.Empty;

        public int Level { get; set; }
    }

    private sealed class Child
    {
        public int Id { get; set; }

        public int ParentId { get; set; }

        public Parent? Parent { get; set; }
    }
}
