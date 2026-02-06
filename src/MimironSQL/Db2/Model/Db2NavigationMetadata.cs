using System.Reflection;

namespace MimironSQL.Db2.Model;

public sealed class Db2NavigationMetadata(MemberInfo navigationMember, Type targetClrType)
{
    public MemberInfo NavigationMember { get; } = navigationMember;
    public Type TargetClrType { get; } = targetClrType;

    public Db2ReferenceNavigationKind? Kind { get; set; }

    public MemberInfo? SourceKeyMember { get; set; }
    public MemberInfo? TargetKeyMember { get; set; }

    public bool OverridesSchema { get; set; }
}
