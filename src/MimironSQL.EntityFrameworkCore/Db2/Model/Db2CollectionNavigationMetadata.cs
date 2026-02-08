using System.Reflection;

namespace MimironSQL.Db2.Model;

internal sealed class Db2CollectionNavigationMetadata(MemberInfo navigationMember, Type targetClrType)
{
    public MemberInfo NavigationMember { get; } = navigationMember;
    public Type TargetClrType { get; } = targetClrType;

    public Db2CollectionNavigationKind? Kind { get; set; }

    public MemberInfo? SourceKeyCollectionMember { get; set; }

    public MemberInfo? DependentForeignKeyMember { get; set; }

    public MemberInfo? PrincipalKeyMember { get; set; }

    public bool OverridesSchema { get; set; }
}
