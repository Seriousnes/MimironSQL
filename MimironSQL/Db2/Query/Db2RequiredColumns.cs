using System.Linq;
using System.Reflection;

using MimironSQL.Db2.Schema;

namespace MimironSQL.Db2.Query;

internal enum Db2RequiredColumnKind
{
    Scalar = 0,
    String,
    JoinKey,
}

internal readonly record struct Db2RequiredColumn(Db2FieldSchema Field, Db2RequiredColumnKind Kind);

internal sealed class Db2SourceRequirements(Db2TableSchema schema, Type clrType)
{
    public Db2TableSchema Schema { get; } = schema;

    public Type ClrType { get; } = clrType;

    public HashSet<Db2RequiredColumn> Columns { get; } = [];

    public void RequireMember(MemberInfo member, Db2RequiredColumnKind kind)
    {
        ArgumentNullException.ThrowIfNull(member);

        if (Schema.TryGetField(member.Name, out var field))
        {
            Columns.Add(new Db2RequiredColumn(field, kind));
            return;
        }

        if (member.Name.Equals("Id", StringComparison.OrdinalIgnoreCase))
        {
            var idField = Schema.Fields.FirstOrDefault(f => f.IsId);
            if (!string.IsNullOrWhiteSpace(idField.Name))
            {
                Columns.Add(new Db2RequiredColumn(idField, kind));
                return;
            }
        }

        throw new NotSupportedException($"Member '{ClrType.FullName}.{member.Name}' was not found in schema '{Schema.TableName}'.");
    }

    public void RequireField(Db2FieldSchema field, Db2RequiredColumnKind kind)
        => Columns.Add(new Db2RequiredColumn(field, kind));
}
