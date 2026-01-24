using System.Reflection;

namespace MimironSQL.Db2.Model;

public sealed class Db2ReferenceNavigation(
    Type sourceClrType,
    MemberInfo navigationMember,
    Type targetClrType,
    Db2ReferenceNavigationKind kind,
    MemberInfo sourceKeyMember,
    MemberInfo targetKeyMember,
    bool overridesSchema)
{
    public Type SourceClrType { get; } = sourceClrType;
    public MemberInfo NavigationMember { get; } = navigationMember;
    public Type TargetClrType { get; } = targetClrType;
    public Db2ReferenceNavigationKind Kind { get; } = kind;

    public MemberInfo SourceKeyMember { get; } = sourceKeyMember;
    public MemberInfo TargetKeyMember { get; } = targetKeyMember;
    public bool OverridesSchema { get; } = overridesSchema;
}
