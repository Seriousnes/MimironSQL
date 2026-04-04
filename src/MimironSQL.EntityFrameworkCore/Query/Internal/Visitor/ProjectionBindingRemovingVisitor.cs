using System.Linq.Expressions;

using Microsoft.EntityFrameworkCore.Query;

namespace MimironSQL.EntityFrameworkCore.Query.Internal.Visitor;

internal sealed class ProjectionBindingRemovingVisitor(ParameterExpression valueBufferParameter) : ExpressionVisitor
{
    private readonly ParameterExpression _valueBufferParameter = valueBufferParameter;

    protected override Expression VisitExtension(Expression node)
    {
        if (node is ProjectionBindingExpression)
        {
            return _valueBufferParameter;
        }

        return base.VisitExtension(node);
    }
}
