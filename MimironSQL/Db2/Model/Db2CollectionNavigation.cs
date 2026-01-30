using System.Reflection;

using MimironSQL.Db2.Schema;

namespace MimironSQL.Db2.Model;

public sealed class Db2CollectionNavigation(
    Type sourceClrType,
    MemberInfo navigationMember,
    Type targetClrType,
    Db2CollectionNavigationKind kind,
    MemberInfo? sourceKeyCollectionMember,
    Db2FieldSchema? sourceKeyFieldSchema,
    MemberInfo? dependentForeignKeyMember,
    Db2FieldSchema? dependentForeignKeyFieldSchema,
    MemberInfo? principalKeyMember,
    bool overridesSchema)
{
    public Type SourceClrType { get; } = sourceClrType;
    public MemberInfo NavigationMember { get; } = navigationMember;
    public Type TargetClrType { get; } = targetClrType;
    public Db2CollectionNavigationKind Kind { get; } = kind;

    public MemberInfo? SourceKeyCollectionMember { get; } = sourceKeyCollectionMember;
    public Db2FieldSchema? SourceKeyFieldSchema { get; } = sourceKeyFieldSchema;

    public MemberInfo? DependentForeignKeyMember { get; } = dependentForeignKeyMember;
    public Db2FieldSchema? DependentForeignKeyFieldSchema { get; } = dependentForeignKeyFieldSchema;

    public MemberInfo? PrincipalKeyMember { get; } = principalKeyMember;
    public bool OverridesSchema { get; } = overridesSchema;
}
