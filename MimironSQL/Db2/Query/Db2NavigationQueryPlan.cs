using System.Reflection;

using MimironSQL.Db2.Model;
using MimironSQL.Db2.Schema;

namespace MimironSQL.Db2.Query;

internal enum Db2NavigationStringMatchKind
{
    Equals = 0,
    Contains,
    StartsWith,
    EndsWith,
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

internal sealed record Db2NavigationMemberAccessPlan(
    Db2NavigationJoinPlan Join,
    MemberInfo TargetMember,
    Db2SourceRequirements RootRequirements,
    Db2SourceRequirements TargetRequirements);

internal sealed record Db2NavigationStringPredicatePlan(
    Db2NavigationJoinPlan Join,
    MemberInfo TargetStringMember,
    Db2FieldSchema TargetStringFieldSchema,
    Db2NavigationStringMatchKind MatchKind,
    string Needle,
    Db2SourceRequirements RootRequirements,
    Db2SourceRequirements TargetRequirements);
