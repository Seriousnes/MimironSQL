using System.Linq.Expressions;
using System.Reflection;

using Microsoft.EntityFrameworkCore.Query;

namespace MimironSQL.EntityFrameworkCore.Db2.Query.Visitor;

internal sealed class QueryParameterRemovingVisitor(ParameterExpression queryContextParameter) : ExpressionVisitor
{
    private readonly ParameterExpression _queryContextParameter = queryContextParameter;

    protected override Expression VisitExtension(Expression node)
    {
        // EF Core represents runtime parameter values (including captured values and compiled query parameters)
        // as QueryParameterExpression, which is not reducible and cannot be compiled directly.
        if (node.GetType().Name == "QueryParameterExpression")
        {
            return Expression.Convert(
                Expression.Call(EvaluateQueryParameterExpressionMethodInfo, Expression.Constant(node, typeof(Expression)), _queryContextParameter),
                node.Type);
        }

        return base.VisitExtension(node);
    }

    private static readonly MethodInfo EvaluateQueryParameterExpressionMethodInfo = typeof(QueryParameterRemovingVisitor)
        .GetTypeInfo()
        .GetDeclaredMethod(nameof(EvaluateQueryParameterExpression))!;

    private static object? EvaluateQueryParameterExpression(Expression queryParameterExpression, QueryContext queryContext)
    {
        var name = GetQueryParameterName(queryParameterExpression);
        if (TryGetQueryContextParameterValues(queryContext, name, out var value))
            return value;

        throw new NotSupportedException(
            $"MimironDb2 could not read query parameter '{name}' from QueryContext during bootstrap execution.");
    }

    private static string GetQueryParameterName(Expression queryParameterExpression)
    {
        const BindingFlags InstanceAnyVisibility = BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic;

        var t = queryParameterExpression.GetType();
        var nameProperty = t.GetProperty("Name", InstanceAnyVisibility);
        var nameField = t.GetField("_name", InstanceAnyVisibility) ?? t.GetField("Name", InstanceAnyVisibility);

        var name = nameProperty?.GetValue(queryParameterExpression) as string
            ?? nameField?.GetValue(queryParameterExpression) as string;

        if (string.IsNullOrWhiteSpace(name))
            throw new NotSupportedException($"MimironDb2 could not read parameter name from QueryParameterExpression type '{t.FullName}'.");

        return name;
    }

    private static bool TryGetQueryContextParameterValues(QueryContext queryContext, string parameterName, out object? value)
    {
        const BindingFlags InstanceAnyVisibility = BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic;

        // Newer EF Core versions sometimes move/rename the parameter store. Be resilient by scanning
        // all instance fields/properties for any dictionary-like storage.
        for (var t = queryContext.GetType(); t is not null; t = t.BaseType)
        {
            foreach (var p in t.GetProperties(InstanceAnyVisibility))
            {
                if (p.GetIndexParameters().Length != 0)
                    continue;

                object? candidate;
                try
                {
                    candidate = p.GetValue(queryContext);
                }
                catch
                {
                    continue;
                }

                if (TryReadFromDictionary(candidate, parameterName, out value))
                    return true;
            }

            foreach (var f in t.GetFields(InstanceAnyVisibility))
            {
                object? candidate;
                try
                {
                    candidate = f.GetValue(queryContext);
                }
                catch
                {
                    continue;
                }

                if (TryReadFromDictionary(candidate, parameterName, out value))
                    return true;
            }
        }

        value = null;
        return false;
    }

    private static bool TryReadFromDictionary(object? candidate, string key, out object? value)
    {
        if (candidate is IReadOnlyDictionary<string, object?> ro)
            return ro.TryGetValue(key, out value);

        if (candidate is IDictionary<string, object?> rw)
        {
            if (rw.TryGetValue(key, out value))
                return true;

            value = null;
            return false;
        }

        if (candidate is System.Collections.IDictionary nongeneric)
        {
            if (nongeneric.Contains(key))
            {
                value = nongeneric[key];
                return true;
            }

            value = null;
            return false;
        }

        value = null;
        return false;
    }
}
