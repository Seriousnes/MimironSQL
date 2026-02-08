using System.Linq.Expressions;

namespace MimironSQL.EntityFrameworkCore.Query;

internal static class MimironDb2EfExpressionNormalizer
{
    public static Expression Normalize(Expression expression)
    {
        ArgumentNullException.ThrowIfNull(expression);
        return new Visitor().Visit(expression);
    }

    private sealed class Visitor : ExpressionVisitor
    {
        protected override Expression VisitMethodCall(MethodCallExpression node)
        {
            if (node.Method.DeclaringType == typeof(Queryable)
                && node.Method.Name == nameof(Queryable.AsQueryable)
                && node.Arguments.Count == 1)
            {
                var visited = Visit(node.Arguments[0]);

                if (typeof(IQueryable).IsAssignableFrom(visited.Type))
                    return visited;

                return node.Update(node.Object, [visited]);
            }

            return base.VisitMethodCall(node);
        }

        protected override Expression VisitUnary(UnaryExpression node)
        {
            var operand = Visit(node.Operand);

            if (node.NodeType is ExpressionType.Convert or ExpressionType.ConvertChecked
                && operand.Type == node.Type)
            {
                return operand;
            }

            return node.Update(operand);
        }
    }
}
