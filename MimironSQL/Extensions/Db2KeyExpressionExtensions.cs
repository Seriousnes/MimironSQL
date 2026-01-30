using System.Linq.Expressions;
using System.Reflection;

namespace MimironSQL.Extensions;

internal static class Db2KeyExpressionExtensions
{
    public static Expression CreateInt32KeyExpression(this MemberInfo member, ParameterExpression instance)
        => CreateMemberAccessExpression(member, instance).ToInt32KeyExpression();

    public static Expression ToInt32KeyExpression(this MemberExpression expression)
    {
        var type = expression.Type;

        if (type == typeof(int))
            return expression;

        if (type == typeof(long))
            return Expression.Call(typeof(Convert).GetMethod(nameof(Convert.ToInt32), [typeof(long)])!, expression);

        if (type == typeof(uint))
            return Expression.Call(typeof(Convert).GetMethod(nameof(Convert.ToInt32), [typeof(uint)])!, expression);

        if (type == typeof(ulong))
            return Expression.Call(typeof(Convert).GetMethod(nameof(Convert.ToInt32), [typeof(ulong)])!, expression);

        if (type == typeof(short) || type == typeof(ushort) || type == typeof(byte) || type == typeof(sbyte))
            return Expression.Convert(expression, typeof(int));

        if (type.IsEnum)
            return Expression.Convert(Expression.Convert(expression, Enum.GetUnderlyingType(type)), typeof(int));

        throw new NotSupportedException($"Unsupported key member type {type.FullName}.");
    }

    private static MemberExpression CreateMemberAccessExpression(MemberInfo member, ParameterExpression instance)
        => member switch
        {
            PropertyInfo p => Expression.Property(instance, p),
            FieldInfo f => Expression.Field(instance, f),
            _ => throw new InvalidOperationException($"Unexpected member type: {member.GetType().FullName}"),
        };
}
