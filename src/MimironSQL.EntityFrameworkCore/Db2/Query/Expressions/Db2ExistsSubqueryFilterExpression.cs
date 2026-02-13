using System.Linq.Expressions;

namespace MimironSQL.EntityFrameworkCore.Db2.Query.Expressions;

/// <summary>
/// Represents an EXISTS subquery filter for collection navigation predicates.
/// Translates patterns like <c>x.Collection.Any()</c> or <c>x.Collection.Count > 0</c>.
/// </summary>
internal sealed class Db2ExistsSubqueryFilterExpression(
    string relatedTableName,
    string foreignKeyColumnName,
    Db2FieldAccessExpression principalKeyField,
    Db2FilterExpression? innerPredicate = null) : Db2FilterExpression
{
    /// <summary>
    /// The table name of the related (dependent) entity.
    /// </summary>
    public string RelatedTableName { get; } = relatedTableName;

    /// <summary>
    /// The column name in the related table that references the principal's key.
    /// </summary>
    public string ForeignKeyColumnName { get; } = foreignKeyColumnName;

    /// <summary>
    /// Field access for the principal entity's key (usually the Id field).
    /// </summary>
    public Db2FieldAccessExpression PrincipalKeyField { get; } = principalKeyField;

    /// <summary>
    /// Optional inner predicate for <c>.Any(predicate)</c> calls.
    /// When null, represents a simple existence check.
    /// </summary>
    public Db2FilterExpression? InnerPredicate { get; } = innerPredicate;

    protected override Expression VisitChildren(ExpressionVisitor visitor) => this;

    public override string ToString()
        => InnerPredicate is null
            ? $"EXISTS ({RelatedTableName} WHERE {ForeignKeyColumnName} = {PrincipalKeyField})"
            : $"EXISTS ({RelatedTableName} WHERE {ForeignKeyColumnName} = {PrincipalKeyField} AND {InnerPredicate})";
}
