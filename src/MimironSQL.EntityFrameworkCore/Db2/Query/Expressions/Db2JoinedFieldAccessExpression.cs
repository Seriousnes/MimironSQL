using System.Linq.Expressions;

using MimironSQL.EntityFrameworkCore.Db2.Schema;

namespace MimironSQL.EntityFrameworkCore.Db2.Query.Expressions;

/// <summary>
/// Represents read access to a field on the inner (joined) table in a join operation.
/// Distinguished from <see cref="Db2FieldAccessExpression"/> which accesses the outer table.
/// </summary>
internal sealed class Db2JoinedFieldAccessExpression(
    string innerTableName,
    Db2FieldSchema field,
    int fieldIndex,
    Type clrType) : Expression
{
    /// <summary>
    /// The table name of the inner (joined) table.
    /// </summary>
    public string InnerTableName { get; } = innerTableName;

    public new Db2FieldSchema Field { get; } = field;
    public int FieldIndex { get; } = fieldIndex;

    public override Type Type { get; } = clrType;
    public override ExpressionType NodeType => ExpressionType.Extension;

    protected override Expression VisitChildren(ExpressionVisitor visitor) => this;

    public override string ToString() => $"ReadJoinedField<{Type.Name}>({InnerTableName}.{Field.Name}[{FieldIndex}])";
}
