using System.Linq.Expressions;

namespace MimironSQL.EntityFrameworkCore.Query.Internal.Visitor;

internal sealed class ParameterReplaceVisitor(ParameterExpression parameter, Expression replacement) : ExpressionVisitor
{
    private readonly ParameterExpression _parameter = parameter;
    private readonly Expression _replacement = replacement;

    protected override Expression VisitParameter(ParameterExpression node)
        => node == _parameter ? _replacement : base.VisitParameter(node);
}
