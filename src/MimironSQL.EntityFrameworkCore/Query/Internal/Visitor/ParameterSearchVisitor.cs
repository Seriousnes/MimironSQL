using System.Linq.Expressions;

namespace MimironSQL.EntityFrameworkCore.Query.Internal.Visitor;

internal sealed class ParameterSearchVisitor(ParameterExpression parameter) : ExpressionVisitor
{
    private readonly ParameterExpression _parameter = parameter;
    private bool _found;

    public static bool Contains(Expression expression, ParameterExpression parameter)
        => new ParameterSearchVisitor(parameter).Contains(expression);

    public bool Contains(Expression expression)
    {
        _found = false;
        Visit(expression);
        return _found;
    }

    protected override Expression VisitParameter(ParameterExpression node)
    {
        if (node == _parameter)
            _found = true;

        return base.VisitParameter(node);
    }
}
