using System.Linq.Expressions;
using System.Reflection;

using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Query;

namespace MimironSQL.EntityFrameworkCore.Query.Internal.Visitor;

internal sealed class EfPropertyRemovingVisitor(ParameterExpression queryContextParameter) : ExpressionVisitor
{
    private readonly ParameterExpression _queryContextParameter = queryContextParameter;

    protected override Expression VisitMethodCall(MethodCallExpression node)
    {
        // EF.Property<TProperty>(object instance, string propertyName)
        if (node.Method.DeclaringType == typeof(EF)
            && node.Method.Name == nameof(EF.Property)
            && node.Method.IsGenericMethod
            && node.Arguments.Count == 2)
        {
            var propertyType = node.Method.GetGenericArguments()[0];
            var instance = Visit(node.Arguments[0]);
            var propertyName = Visit(node.Arguments[1]);

            if (propertyName.Type != typeof(string))
                propertyName = Expression.Convert(propertyName, typeof(string));

            return Expression.Call(
                EvaluateEfPropertyMethodInfo.MakeGenericMethod(propertyType),
                _queryContextParameter,
                Expression.Convert(instance, typeof(object)),
                propertyName);
        }

        return base.VisitMethodCall(node);
    }

    private static readonly MethodInfo EvaluateEfPropertyMethodInfo = typeof(EfPropertyRemovingVisitor)
        .GetTypeInfo()
        .GetDeclaredMethod(nameof(EvaluateEfProperty))!;

    private static TProperty? EvaluateEfProperty<TProperty>(QueryContext queryContext, object entity, string propertyName)
    {
        ArgumentNullException.ThrowIfNull(queryContext);
        ArgumentNullException.ThrowIfNull(entity);
        ArgumentException.ThrowIfNullOrWhiteSpace(propertyName);

        // Prefer CLR member access for speed.
        const BindingFlags InstanceAnyVisibility = BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic;
        var t = entity.GetType();

        var p = t.GetProperty(propertyName, InstanceAnyVisibility);
        if (p is not null)
            return (TProperty?)p.GetValue(entity);

        var f = t.GetField(propertyName, InstanceAnyVisibility);
        if (f is not null)
            return (TProperty?)f.GetValue(entity);

        // Shadow property fallback: use EF Core's entry APIs.
        var dbContext = GetDbContext(queryContext);
        var entry = dbContext.Entry(entity);
        return (TProperty?)entry.Property(propertyName).CurrentValue;
    }

    private static DbContext GetDbContext(QueryContext queryContext)
    {
        // QueryContext.Context is public in EF Core, but use reflection for resiliency across versions.
        const BindingFlags InstanceAnyVisibility = BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic;

        var t = queryContext.GetType();
        var contextProperty = t.GetProperty("Context", InstanceAnyVisibility);
        if (contextProperty?.GetValue(queryContext) is DbContext dbContext)
            return dbContext;

        throw new NotSupportedException(
            $"MimironDb2 could not read DbContext from QueryContext type '{t.FullName}' while evaluating EF.Property().");
    }
}
