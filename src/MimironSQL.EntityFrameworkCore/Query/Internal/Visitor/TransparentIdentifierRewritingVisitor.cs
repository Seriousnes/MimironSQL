using System.Linq.Expressions;

namespace MimironSQL.EntityFrameworkCore.Query.Internal.Visitor;

internal sealed class TransparentIdentifierRewritingVisitor(
    ParameterExpression transparentParameter,
    Expression outerReplacement,
    Expression innerReplacement) : ExpressionVisitor
{
    private readonly ParameterExpression _transparentParameter = transparentParameter;
    private readonly Expression _outerReplacement = outerReplacement;
    private readonly Expression _innerReplacement = innerReplacement;

    protected override Expression VisitMember(MemberExpression node)
    {
        if (node.Expression == _transparentParameter)
        {
            if (node.Member.Name == "Outer")
                return _outerReplacement;

            if (node.Member.Name == "Inner")
                return _innerReplacement;
        }

        return base.VisitMember(node);
    }
}
