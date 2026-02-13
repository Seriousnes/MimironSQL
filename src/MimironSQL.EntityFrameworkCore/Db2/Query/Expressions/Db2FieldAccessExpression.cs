using System.Linq.Expressions;

using MimironSQL.EntityFrameworkCore.Db2.Schema;

namespace MimironSQL.EntityFrameworkCore.Db2.Query.Expressions;

/// <summary>
/// Represents read access to a DB2 field by column index.
/// </summary>
internal sealed class Db2FieldAccessExpression(
    Db2FieldSchema field,
    int fieldIndex,
    Type clrType) : Expression
{
    public new Db2FieldSchema Field { get; } = field;
    public int FieldIndex { get; } = fieldIndex;

    public override Type Type { get; } = clrType;
    public override ExpressionType NodeType => ExpressionType.Extension;

    protected override Expression VisitChildren(ExpressionVisitor visitor) => this;

    public override string ToString() => $"ReadField<{Type.Name}>({Field.Name}[{FieldIndex}])";
}
