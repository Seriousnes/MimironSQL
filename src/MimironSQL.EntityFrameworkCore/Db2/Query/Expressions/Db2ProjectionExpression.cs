using System.Linq.Expressions;

namespace MimironSQL.EntityFrameworkCore.Db2.Query.Expressions;

/// <summary>
/// Represents a projected field in a DB2 query.
/// </summary>
internal sealed class Db2ProjectionExpression(
    Db2FieldAccessExpression field,
    string alias) : Expression
{
    public new Db2FieldAccessExpression Field { get; } = field;
    public string Alias { get; } = alias;

    public override Type Type => Field.Type;
    public override ExpressionType NodeType => ExpressionType.Extension;

    protected override Expression VisitChildren(ExpressionVisitor visitor) => this;

    public override string ToString() => $"{Field} AS {Alias}";
}
