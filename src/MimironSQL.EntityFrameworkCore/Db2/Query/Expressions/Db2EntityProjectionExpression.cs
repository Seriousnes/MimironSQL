using System.Linq.Expressions;

using Microsoft.EntityFrameworkCore.Metadata;

namespace MimironSQL.EntityFrameworkCore.Db2.Query.Expressions;

/// <summary>
/// Represents a projection of all properties for an entity type.
/// This is a sentinel used in the projection mapping before compilation finalizes projections.
/// Analogous to InMemory's <c>EntityProjectionExpression</c>.
/// </summary>
internal sealed class Db2EntityProjectionExpression(IEntityType entityType) : Expression
{
    public IEntityType EntityType { get; } = entityType;

    public override Type Type => EntityType.ClrType;
    public override ExpressionType NodeType => ExpressionType.Extension;

    protected override Expression VisitChildren(ExpressionVisitor visitor) => this;

    public override string ToString() => $"EntityProjection({EntityType.DisplayName()})";
}
