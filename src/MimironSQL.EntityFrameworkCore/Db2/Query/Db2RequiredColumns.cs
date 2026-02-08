using System.Reflection;

using MimironSQL.EntityFrameworkCore.Db2.Model;
using MimironSQL.EntityFrameworkCore.Db2.Schema;

namespace MimironSQL.EntityFrameworkCore.Db2.Query;

internal enum Db2RequiredColumnKind
{
    Scalar = 0,
    String,
    JoinKey,
}

internal readonly record struct Db2RequiredColumn(Db2FieldSchema Field, Db2RequiredColumnKind Kind);

internal sealed class Db2SourceRequirements(Db2EntityType entityType)
{
    public Db2EntityType EntityType { get; } = entityType;

    public Db2TableSchema Schema => EntityType.Schema;

    public Type ClrType => EntityType.ClrType;

    public HashSet<Db2RequiredColumn> Columns { get; } = [];

    public void RequireMember(MemberInfo member, Db2RequiredColumnKind kind)
    {
        ArgumentNullException.ThrowIfNull(member);

        var field = EntityType.ResolveFieldSchema(member, context: $"requirements for '{ClrType.FullName}.{member.Name}'");
        Columns.Add(new Db2RequiredColumn(field, kind));
    }

    public void RequireField(Db2FieldSchema field, Db2RequiredColumnKind kind)
        => Columns.Add(new Db2RequiredColumn(field, kind));
}
