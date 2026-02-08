using System.Linq.Expressions;

namespace MimironSQL.EntityFrameworkCore.Extensions;

internal static class ExpressionExtensions
{
    public static Expression? UnwrapConvert(this Expression? expression)
        => expression is UnaryExpression { NodeType: ExpressionType.Convert or ExpressionType.ConvertChecked } u
            ? u.Operand
            : expression;

    public static bool ContainsParameter(this Expression expression, ParameterExpression parameter)
    {
        var found = false;

        void Visit(Expression? e)
        {
            if (found || e is null)
                return;

            e = e.UnwrapConvert();

            if (ReferenceEquals(e, parameter))
            {
                found = true;
                return;
            }

            switch (e)
            {
                case MemberExpression me:
                    Visit(me.Expression);
                    return;
                case MethodCallExpression mc:
                    Visit(mc.Object);
                    foreach (var a in mc.Arguments)
                        Visit(a);
                    return;
                case UnaryExpression u:
                    Visit(u.Operand);
                    return;
                case BinaryExpression b:
                    Visit(b.Left);
                    Visit(b.Right);
                    return;
                case ConditionalExpression c:
                    Visit(c.Test);
                    Visit(c.IfTrue);
                    Visit(c.IfFalse);
                    return;
                case NewExpression n:
                    foreach (var a in n.Arguments)
                        Visit(a);
                    return;
                case MemberInitExpression mi:
                    Visit(mi.NewExpression);
                    foreach (var bnd in mi.Bindings)
                    {
                        if (bnd is MemberAssignment ma)
                            Visit(ma.Expression);
                    }
                    return;
                default:
                    return;
            }
        }

        Visit(expression);
        return found;
    }
}
