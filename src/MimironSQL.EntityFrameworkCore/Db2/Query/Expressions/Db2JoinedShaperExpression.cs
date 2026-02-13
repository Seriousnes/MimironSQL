using System.Linq.Expressions;

namespace MimironSQL.EntityFrameworkCore.Db2.Query.Expressions;

/// <summary>
/// Represents a shaper expression for a joined query result.
/// Tracks both the outer and inner shapers for materialization.
/// </summary>
internal sealed class Db2JoinedShaperExpression : Expression
{
    public Db2JoinedShaperExpression(
        Expression outerShaper,
        Expression innerShaper,
        LambdaExpression resultSelector)
    {
        OuterShaper = outerShaper;
        InnerShaper = innerShaper;
        ResultSelector = resultSelector;
        Type = resultSelector.ReturnType;
    }

    /// <summary>
    /// Shaper expression for the outer (principal) entity.
    /// </summary>
    public Expression OuterShaper { get; }

    /// <summary>
    /// Shaper expression for the inner (related) entity.
    /// </summary>
    public Expression InnerShaper { get; }

    /// <summary>
    /// The result selector that combines outer and inner.
    /// </summary>
    public LambdaExpression ResultSelector { get; }

    public override Type Type { get; }
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
