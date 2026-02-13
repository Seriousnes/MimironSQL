namespace MimironSQL.EntityFrameworkCore.Db2.Query.Expressions;

/// <summary>
/// Represents an ordering clause in a DB2 query.
/// </summary>
internal sealed class Db2OrderingExpression(
    Db2FieldAccessExpression field,
    bool ascending)
{
    public Db2FieldAccessExpression Field { get; } = field;
    public bool Ascending { get; } = ascending;

    public override string ToString() => $"{Field} {(Ascending ? "ASC" : "DESC")}";
}
