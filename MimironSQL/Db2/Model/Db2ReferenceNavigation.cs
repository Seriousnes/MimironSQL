using System.Reflection;

using MimironSQL.Db2.Schema;

namespace MimironSQL.Db2.Model;

public sealed class Db2ReferenceNavigation(
    Type sourceClrType,
    MemberInfo navigationMember,
    Type targetClrType,
    Db2ReferenceNavigationKind kind,
    MemberInfo sourceKeyMember,
    MemberInfo targetKeyMember,
    Db2FieldSchema sourceKeyFieldSchema,
    Db2FieldSchema targetKeyFieldSchema,
    bool overridesSchema)
{
    public Type SourceClrType { get; } = sourceClrType;
    public MemberInfo NavigationMember { get; } = navigationMember;
    public Type TargetClrType { get; } = targetClrType;
    public Db2ReferenceNavigationKind Kind { get; } = kind;

    public MemberInfo SourceKeyMember { get; } = sourceKeyMember;
    public MemberInfo TargetKeyMember { get; } = targetKeyMember;
    public Db2FieldSchema SourceKeyFieldSchema { get; } = sourceKeyFieldSchema;
    public Db2FieldSchema TargetKeyFieldSchema { get; } = targetKeyFieldSchema;
    public bool OverridesSchema { get; } = overridesSchema;
}
