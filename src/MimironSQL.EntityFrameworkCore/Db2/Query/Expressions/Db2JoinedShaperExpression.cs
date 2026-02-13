using System.Linq.Expressions;

namespace MimironSQL.EntityFrameworkCore.Db2.Query.Expressions;

/// <summary>
/// Represents a shaper expression for a joined query result.
/// Tracks both the outer and inner shapers for materialization.
/// </summary>
internal sealed class Db2JoinedShaperExpression(
    Expression outerShaper,
    Expression innerShaper,
    LambdaExpression resultSelector) : Expression
{

    /// <summary>
    /// Shaper expression for the outer (principal) entity.
    /// </summary>
    public Expression OuterShaper { get; } = outerShaper;

    /// <summary>
    /// Shaper expression for the inner (related) entity.
    /// </summary>
    public Expression InnerShaper { get; } = innerShaper;

    /// <summary>
    /// The result selector that combines outer and inner.
    /// </summary>
    public LambdaExpression ResultSelector { get; } = resultSelector;

    public override Type Type { get; } = resultSelector.ReturnType;
    public override ExpressionType NodeType => ExpressionType.Extension;

    protected override Expression VisitChildren(ExpressionVisitor visitor)
    {
        var visitedOuter = visitor.Visit(OuterShaper);
        var visitedInner = visitor.Visit(InnerShaper);

        if (visitedOuter != OuterShaper || visitedInner != InnerShaper)
        {
            return new Db2JoinedShaperExpression(visitedOuter, visitedInner, ResultSelector);
        }

        return this;
    }
}
