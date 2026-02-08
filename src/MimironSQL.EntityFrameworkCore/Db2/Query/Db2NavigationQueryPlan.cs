using System.Reflection;

using MimironSQL.EntityFrameworkCore.Db2.Model;
using MimironSQL.EntityFrameworkCore.Db2.Schema;

namespace MimironSQL.EntityFrameworkCore.Db2.Query;

internal enum Db2NavigationStringMatchKind
{
    Equals = 0,
    Contains,
    StartsWith,
    EndsWith,
}

internal enum Db2ScalarComparisonKind
{
    Equal = 0,
    NotEqual,
    LessThan,
    LessThanOrEqual,
    GreaterThan,
    GreaterThanOrEqual,
}

internal sealed record Db2NavigationJoinPlan(
    Db2EntityType Root,
    Db2ReferenceNavigation Navigation,
    Db2EntityType Target)
{
    public Db2ReferenceNavigationKind Kind => Navigation.Kind;

    public MemberInfo RootKeyMember => Navigation.Kind switch
    {
        Db2ReferenceNavigationKind.ForeignKeyToPrimaryKey => Navigation.SourceKeyMember,
        Db2ReferenceNavigationKind.SharedPrimaryKeyOneToOne => Root.PrimaryKeyMember,
        _ => throw new NotSupportedException($"Unsupported navigation kind '{Navigation.Kind}'."),
    };

    public MemberInfo TargetKeyMember => Navigation.Kind switch
    {
        Db2ReferenceNavigationKind.ForeignKeyToPrimaryKey => Navigation.TargetKeyMember,
        Db2ReferenceNavigationKind.SharedPrimaryKeyOneToOne => Target.PrimaryKeyMember,
        _ => throw new NotSupportedException($"Unsupported navigation kind '{Navigation.Kind}'."),
    };

    public Db2FieldSchema RootKeyFieldSchema => Navigation.Kind switch
    {
        Db2ReferenceNavigationKind.ForeignKeyToPrimaryKey => Navigation.SourceKeyFieldSchema,
        Db2ReferenceNavigationKind.SharedPrimaryKeyOneToOne => Root.PrimaryKeyFieldSchema,
        _ => throw new NotSupportedException($"Unsupported navigation kind '{Navigation.Kind}'."),
    };

    public Db2FieldSchema TargetKeyFieldSchema => Navigation.Kind switch
    {
        Db2ReferenceNavigationKind.ForeignKeyToPrimaryKey => Navigation.TargetKeyFieldSchema,
        Db2ReferenceNavigationKind.SharedPrimaryKeyOneToOne => Target.PrimaryKeyFieldSchema,
        _ => throw new NotSupportedException($"Unsupported navigation kind '{Navigation.Kind}'."),
    };
}

internal sealed record Db2NavigationStringPredicatePlan(
    Db2NavigationJoinPlan Join,
    MemberInfo TargetStringMember,
    Db2FieldSchema TargetStringFieldSchema,
    Db2NavigationStringMatchKind MatchKind,
    string Needle,
    Db2SourceRequirements RootRequirements,
    Db2SourceRequirements TargetRequirements);

internal abstract record Db2NavigationScalarPredicatePlan(
    Db2NavigationJoinPlan Join,
    MemberInfo TargetScalarMember,
    Db2FieldSchema TargetScalarFieldSchema,
    Db2ScalarComparisonKind ComparisonKind,
    Db2SourceRequirements RootRequirements,
    Db2SourceRequirements TargetRequirements);

internal sealed record Db2NavigationScalarPredicatePlan<T>(
    Db2NavigationJoinPlan Join,
    MemberInfo TargetScalarMember,
    Db2FieldSchema TargetScalarFieldSchema,
    Db2ScalarComparisonKind ComparisonKind,
    T ComparisonValue,
    Db2SourceRequirements RootRequirements,
    Db2SourceRequirements TargetRequirements)
    : Db2NavigationScalarPredicatePlan(
        Join,
        TargetScalarMember,
        TargetScalarFieldSchema,
        ComparisonKind,
        RootRequirements,
        TargetRequirements)
    where T : unmanaged, IComparable<T>, IEquatable<T>;

internal sealed record Db2NavigationNullCheckPlan(
    Db2NavigationJoinPlan Join,
    bool IsNotNull,
    Db2SourceRequirements RootRequirements,
    Db2SourceRequirements TargetRequirements);
