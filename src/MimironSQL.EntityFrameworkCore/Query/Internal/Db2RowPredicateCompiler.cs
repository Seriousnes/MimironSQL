using System.Linq.Expressions;
using System.Reflection;

using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Query;

using MimironSQL.Db2;
using MimironSQL.Formats;
using MimironSQL.EntityFrameworkCore.Query.Internal.Visitor;
using MimironSQL.EntityFrameworkCore.Schema;

namespace MimironSQL.EntityFrameworkCore.Query.Internal;

internal static class Db2RowPredicateCompiler
{
    internal static bool TryGetTransparentIdentifierTypes(Type type, out Type outer, out Type inner)
    {
        if (type.IsGenericType
            && type.DeclaringType is not null
            && type.DeclaringType.Name == "TransparentIdentifierFactory"
            && type.Name.StartsWith("TransparentIdentifier`", StringComparison.Ordinal)
            && type.GetGenericArguments() is { Length: 2 } args)
        {
            outer = args[0];
            inner = args[1];
            return true;
        }

        outer = null!;
        inner = null!;
        return false;
    }

    internal static IEnumerable<Expression> SplitAndAlso(Expression expression)
    {
        while (expression is UnaryExpression { NodeType: ExpressionType.Convert or ExpressionType.ConvertChecked } u)
            expression = u.Operand;

        if (expression is BinaryExpression { NodeType: ExpressionType.AndAlso, Left: var left, Right: var right })
        {
            foreach (var l in SplitAndAlso(left))
                yield return l;
            foreach (var r in SplitAndAlso(right))
                yield return r;
            yield break;
        }

        yield return expression;
    }

    internal static Func<QueryContext, IDb2File, RowHandle, bool>? TryCompileRowHandlePredicate(
        IEntityType entityType,
        string tableName,
        Db2TableSchema schema,
        IReadOnlyList<LambdaExpression> predicates)
    {
        if (predicates.Count == 0)
            return null;

        var entityClrType = entityType.ClrType;

        var queryContextParameter = Expression.Parameter(typeof(QueryContext), "qc");
        var fileParameter = Expression.Parameter(typeof(IDb2File), "file");
        var handleParameter = Expression.Parameter(typeof(RowHandle), "handle");
        var entityParameter = Expression.Parameter(entityClrType, "e");

        var storeObject = StoreObjectIdentifier.Table(tableName, schema: null);
        var translatedConjuncts = new List<Expression>();

        foreach (var predicate in predicates)
        {
            if (predicate.Parameters.Count != 1)
                continue;

            var expectedParameterType = predicate.Parameters[0].Type;
            if (TryGetTransparentIdentifierTypes(expectedParameterType, out _, out _))
                continue;

            // Only push down predicates which target the root entity instance.
            if (!expectedParameterType.IsAssignableFrom(entityClrType))
                continue;

            var entityAsExpected = expectedParameterType == entityClrType
                ? (Expression)entityParameter
                : Expression.Convert(entityParameter, expectedParameterType);

            var rewrittenBody = new ParameterReplaceVisitor(predicate.Parameters[0], entityAsExpected).Visit(predicate.Body);
            rewrittenBody = new QueryParameterRemovingVisitor(queryContextParameter).Visit(rewrittenBody!);

            foreach (var conjunct in SplitAndAlso(rewrittenBody!))
            {
                if (TryTranslateConjunct(
                    entityType,
                    storeObject,
                    schema,
                    entityParameter,
                    queryContextParameter,
                    fileParameter,
                    handleParameter,
                    conjunct,
                    out var translated))
                {
                    translatedConjuncts.Add(translated);
                }
            }
        }

        if (translatedConjuncts.Count == 0)
            return null;

        Expression combined = translatedConjuncts[0];
        for (var i = 1; i < translatedConjuncts.Count; i++)
            combined = Expression.AndAlso(combined, translatedConjuncts[i]);

        var lambda = Expression.Lambda<Func<QueryContext, IDb2File, RowHandle, bool>>(
            combined,
            queryContextParameter,
            fileParameter,
            handleParameter);

        return lambda.Compile();
    }

    internal static bool TryTranslateConjunct(
        IEntityType entityType,
        StoreObjectIdentifier storeObject,
        Db2TableSchema schema,
        ParameterExpression entityParameter,
        ParameterExpression queryContextParameter,
        ParameterExpression fileParameter,
        ParameterExpression handleParameter,
        Expression expression,
        out Expression translated)
    {
        translated = null!;

        while (expression is UnaryExpression { NodeType: ExpressionType.Convert or ExpressionType.ConvertChecked } u)
            expression = u.Operand;

        if (expression.Type != typeof(bool))
            return false;

        if (expression is UnaryExpression { NodeType: ExpressionType.Not, Operand: var operand })
        {
            if (!TryTranslateConjunct(entityType, storeObject, schema, entityParameter, queryContextParameter, fileParameter, handleParameter, operand, out var inner))
                return false;

            translated = Expression.Not(inner);
            return true;
        }

        if (expression is BinaryExpression b)
        {
            if (b.NodeType is ExpressionType.AndAlso or ExpressionType.OrElse)
            {
                if (!TryTranslateConjunct(entityType, storeObject, schema, entityParameter, queryContextParameter, fileParameter, handleParameter, b.Left, out var left))
                    return false;

                if (!TryTranslateConjunct(entityType, storeObject, schema, entityParameter, queryContextParameter, fileParameter, handleParameter, b.Right, out var right))
                    return false;

                translated = b.NodeType == ExpressionType.AndAlso
                    ? Expression.AndAlso(left, right)
                    : Expression.OrElse(left, right);
                return true;
            }

            if (b.NodeType is ExpressionType.Equal
                or ExpressionType.NotEqual
                or ExpressionType.LessThan
                or ExpressionType.LessThanOrEqual
                or ExpressionType.GreaterThan
                or ExpressionType.GreaterThanOrEqual)
            {
                if (TryTranslateFieldAccess(entityType, storeObject, schema, entityParameter, fileParameter, handleParameter, b.Left, out var leftField)
                    && !ParameterSearchVisitor.Contains(b.Right, entityParameter))
                {
                    translated = RebuildComparison(b.NodeType, leftField, b.Right);
                    return true;
                }

                if (TryTranslateFieldAccess(entityType, storeObject, schema, entityParameter, fileParameter, handleParameter, b.Right, out var rightField)
                    && !ParameterSearchVisitor.Contains(b.Left, entityParameter))
                {
                    translated = RebuildComparison(b.NodeType, rightField, b.Left, swap: true);
                    return true;
                }

                return false;
            }

            return false;
        }

        // string.StartsWith/EndsWith/Contains against a DB2 string field.
        // Must be checked before the IN-list Contains branch, since that branch matches by method name.
        if (expression is MethodCallExpression stringCall
            && stringCall.Method.DeclaringType == typeof(string)
            && stringCall.Method.Name is nameof(string.StartsWith) or nameof(string.EndsWith) or nameof(string.Contains))
        {
            return TryTranslateStringMethodCall(
                entityType,
                storeObject,
                schema,
                entityParameter,
                queryContextParameter,
                fileParameter,
                handleParameter,
                stringCall,
                out translated);
        }

        // Contains/IN against an in-memory set: ids.Contains(e.Id) or Enumerable.Contains(ids, e.Id)
        if (expression is MethodCallExpression call
            && call.Method.Name == nameof(Enumerable.Contains))
        {
            Expression? valuesExpression = null;
            Expression? itemExpression = null;

            if (call.Method.DeclaringType == typeof(Enumerable) && call.Arguments.Count == 2)
            {
                valuesExpression = call.Arguments[0];
                itemExpression = call.Arguments[1];
            }
            else if (!call.Method.IsStatic && call.Arguments.Count == 1)
            {
                valuesExpression = call.Object;
                itemExpression = call.Arguments[0];
            }

            if (valuesExpression is null || itemExpression is null)
                return false;

            // Avoid accidentally treating string.Contains(string) as an IN-list.
            if (valuesExpression.Type == typeof(string))
                return false;

            if (ParameterSearchVisitor.Contains(valuesExpression, entityParameter))
                return false;

            if (!TryTranslateFieldAccess(entityType, storeObject, schema, entityParameter, fileParameter, handleParameter, itemExpression, out var itemTranslated))
                return false;

            var itemType = itemTranslated.Type;

            // Require that values is IEnumerable<T> (or can be converted to it).
            var enumerableOfItem = typeof(IEnumerable<>).MakeGenericType(itemType);
            if (!enumerableOfItem.IsAssignableFrom(valuesExpression.Type))
                return false;

            var contains = typeof(Enumerable)
                .GetMethods(BindingFlags.Public | BindingFlags.Static)
                .Single(m => m.Name == nameof(Enumerable.Contains) && m.GetParameters().Length == 2)
                .MakeGenericMethod(itemType);

            translated = Expression.Call(contains, Expression.Convert(valuesExpression, enumerableOfItem), itemTranslated);
            return true;
        }

        return false;

        static Expression RebuildComparison(ExpressionType op, Expression left, Expression right, bool swap = false)
        {
            // Keep comparison semantics by aligning types.
            // If we swapped operands, also swap relational operators.
            if (swap)
            {
                op = op switch
                {
                    ExpressionType.LessThan => ExpressionType.GreaterThan,
                    ExpressionType.LessThanOrEqual => ExpressionType.GreaterThanOrEqual,
                    ExpressionType.GreaterThan => ExpressionType.LessThan,
                    ExpressionType.GreaterThanOrEqual => ExpressionType.LessThanOrEqual,
                    _ => op,
                };
            }

            if (right.Type != left.Type)
            {
                if (right.Type.IsAssignableFrom(left.Type))
                {
                    right = Expression.Convert(right, left.Type);
                }
                else if (left.Type.IsAssignableFrom(right.Type))
                {
                    left = Expression.Convert(left, right.Type);
                }
                else
                {
                    right = Expression.Convert(right, left.Type);
                }
            }

            return op switch
            {
                ExpressionType.Equal => Expression.Equal(left, right),
                ExpressionType.NotEqual => Expression.NotEqual(left, right),
                ExpressionType.LessThan => Expression.LessThan(left, right),
                ExpressionType.LessThanOrEqual => Expression.LessThanOrEqual(left, right),
                ExpressionType.GreaterThan => Expression.GreaterThan(left, right),
                ExpressionType.GreaterThanOrEqual => Expression.GreaterThanOrEqual(left, right),
                _ => throw new NotSupportedException($"Unsupported comparison operator '{op}'."),
            };
        }
    }

    private static bool TryTranslateStringMethodCall(
        IEntityType entityType,
        StoreObjectIdentifier storeObject,
        Db2TableSchema schema,
        ParameterExpression entityParameter,
        ParameterExpression queryContextParameter,
        ParameterExpression fileParameter,
        ParameterExpression handleParameter,
        MethodCallExpression call,
        out Expression translated)
    {
        translated = null!;

        // Only instance methods on string.
        if (call.Object is null)
            return false;

        if (call.Method.DeclaringType != typeof(string))
            return false;

        if (call.Method.Name is not (nameof(string.StartsWith) or nameof(string.EndsWith) or nameof(string.Contains)))
            return false;

        if (call.Arguments.Count is < 1 or > 2)
            return false;

        if (!TryTranslateFieldAccess(entityType, storeObject, schema, entityParameter, fileParameter, handleParameter, call.Object, out var receiverTranslated))
            return false;

        var valueArgument = call.Arguments[0];
        if (!TryTranslateStringMethodArgument(valueArgument, entityParameter, queryContextParameter, out var valueTranslated))
            return false;

        if (call.Arguments.Count == 1)
        {
            translated = Expression.Call(receiverTranslated, call.Method, valueTranslated);
            return true;
        }

        if (!TryGetConstantStringComparison(call.Arguments[1], out var comparison))
            return false;

        if (comparison is not (StringComparison.Ordinal or StringComparison.OrdinalIgnoreCase))
            return false;

        translated = Expression.Call(receiverTranslated, call.Method, valueTranslated, Expression.Constant(comparison));
        return true;
    }

    private static bool TryTranslateStringMethodArgument(
        Expression argument,
        ParameterExpression entityParameter,
        ParameterExpression queryContextParameter,
        out Expression translated)
    {
        // Allow constants and captured/query parameters (but reject anything that depends on the entity).
        translated = null!;

        if (argument.Type != typeof(string))
            return false;

        if (ParameterSearchVisitor.Contains(argument, entityParameter))
            return false;

        if (IsConstantOrCapturedString(argument, queryContextParameter))
        {
            translated = argument;
            return true;
        }

        return false;
    }

    private static bool IsConstantOrCapturedString(Expression expression, ParameterExpression queryContextParameter)
    {
        while (expression is UnaryExpression { NodeType: ExpressionType.Convert or ExpressionType.ConvertChecked } u)
            expression = u.Operand;

        if (expression is ConstantExpression)
            return true;

        // Captured variable: closure field/property access ("new <>c__DisplayClass" instance constant).
        if (expression is MemberExpression { Expression: ConstantExpression })
            return true;

        // Compiled query parameter / captured value: QueryParameterRemovingVisitor rewrites these into a
        // call to a private helper which reads from QueryContext at runtime.
        if (expression is MethodCallExpression { Method: { Name: "EvaluateQueryParameterExpression", DeclaringType: var decl } } call
            && decl == typeof(QueryParameterRemovingVisitor)
            && ParameterSearchVisitor.Contains(call, queryContextParameter))
        {
            return true;
        }

        return false;
    }

    private static bool TryGetConstantStringComparison(Expression expression, out StringComparison comparison)
    {
        while (expression is UnaryExpression { NodeType: ExpressionType.Convert or ExpressionType.ConvertChecked } u)
            expression = u.Operand;

        if (expression is ConstantExpression { Value: StringComparison sc })
        {
            comparison = sc;
            return true;
        }

        comparison = default;
        return false;
    }

    internal static bool TryTranslateFieldAccess(
        IEntityType entityType,
        StoreObjectIdentifier storeObject,
        Db2TableSchema schema,
        ParameterExpression entityParameter,
        ParameterExpression fileParameter,
        ParameterExpression handleParameter,
        Expression expression,
        out Expression translated)
    {
        translated = null!;

        while (expression is UnaryExpression { NodeType: ExpressionType.Convert or ExpressionType.ConvertChecked } u)
            expression = u.Operand;

        string? propertyName = null;

        if (expression is MemberExpression { Expression: var instance, Member: PropertyInfo pi })
        {
            if (instance is null || !IsEntityInstance(instance, entityParameter))
                return false;
            propertyName = pi.Name;
        }
        else if (expression is MemberExpression { Expression: var instance2, Member: FieldInfo fi })
        {
            if (instance2 is null || !IsEntityInstance(instance2, entityParameter))
                return false;
            propertyName = fi.Name;
        }
        else if (expression is MethodCallExpression
            {
                Method.DeclaringType: var decl,
                Method.Name: nameof(EF.Property),
                Arguments: [var inst, ConstantExpression { Value: string s }]
            }
            && decl == typeof(EF)
            && IsEntityInstance(inst, entityParameter))
        {
            propertyName = s;
        }

        if (string.IsNullOrWhiteSpace(propertyName))
            return false;

        var property = entityType.FindProperty(propertyName);
        if (property is null)
            return false;

        var columnName = property.GetColumnName(storeObject) ?? property.GetColumnName() ?? property.Name;
        if (!schema.TryGetFieldCaseInsensitive(columnName, out var fieldSchema))
            return false;

        var resultType = property.ClrType;
        var readType = GetReadTypeForReadField(resultType);

        var readMethod = typeof(IDb2File)
            .GetMethod(nameof(IDb2File.ReadField), BindingFlags.Instance | BindingFlags.Public)!
            .MakeGenericMethod(readType);

        Expression readCall = Expression.Call(fileParameter, readMethod, handleParameter, Expression.Constant(fieldSchema.ColumnStartIndex));
        if (readType != resultType)
            readCall = Expression.Convert(readCall, resultType);

        translated = readCall;
        return true;

        static bool IsEntityInstance(Expression instance, ParameterExpression entityParameter)
        {
            while (instance is UnaryExpression { NodeType: ExpressionType.Convert or ExpressionType.ConvertChecked } u)
                instance = u.Operand;

            return instance == entityParameter;
        }
    }

    internal static Type GetReadTypeForReadField(Type resultType)
    {
        ArgumentNullException.ThrowIfNull(resultType);

        if (resultType.IsArray)
            return resultType;

        var unwrapped = Nullable.GetUnderlyingType(resultType);
        return unwrapped ?? resultType;
    }

    internal static bool TryGetVirtualIdPrimaryKeyEqualityLookup(
        IEntityType entityType,
        string tableName,
        Db2TableSchema schema,
        IReadOnlyList<LambdaExpression> predicates,
        out Func<QueryContext, object?> getKeyValue,
        out Type keyType)
    {
        getKeyValue = null!;
        keyType = null!;

        if (predicates.Count != 1)
            return false;

        var pk = entityType.FindPrimaryKey();
        if (pk is null || pk.Properties.Count != 1)
            return false;

        var keyProperty = pk.Properties[0];
        var storeObject = StoreObjectIdentifier.Table(tableName, schema: null);
        var keyColumnName = keyProperty.GetColumnName(storeObject) ?? keyProperty.GetColumnName() ?? keyProperty.Name;
        if (!schema.TryGetFieldCaseInsensitive(keyColumnName, out var keyField) || keyField.ColumnStartIndex != Db2VirtualFieldIndex.Id)
            return false;

        var predicate = predicates[0];
        if (predicate.Parameters.Count != 1)
            return false;

        var entityClrType = entityType.ClrType;
        var expectedParameterType = predicate.Parameters[0].Type;
        if (TryGetTransparentIdentifierTypes(expectedParameterType, out _, out _))
            return false;

        if (!expectedParameterType.IsAssignableFrom(entityClrType))
            return false;

        var entityParameter = Expression.Parameter(entityClrType, "e");
        var entityAsExpected = expectedParameterType == entityClrType
            ? (Expression)entityParameter
            : Expression.Convert(entityParameter, expectedParameterType);

        var queryContextParameter = Expression.Parameter(typeof(QueryContext), "qc");
        var rewrittenBody = new ParameterReplaceVisitor(predicate.Parameters[0], entityAsExpected).Visit(predicate.Body);
        rewrittenBody = new QueryParameterRemovingVisitor(queryContextParameter).Visit(rewrittenBody!);

        var conjuncts = SplitAndAlso(rewrittenBody!).ToArray();
        if (conjuncts.Length != 1)
            return false;

        var only = conjuncts[0];
        while (only is UnaryExpression { NodeType: ExpressionType.Convert or ExpressionType.ConvertChecked } u)
            only = u.Operand;

        if (only is not BinaryExpression { NodeType: ExpressionType.Equal, Left: var left, Right: var right })
            return false;

        var matchedLeft = TryMatchPropertyAccess(entityType, entityParameter, left, out var leftProperty);
        var matchedRight = !matchedLeft && TryMatchPropertyAccess(entityType, entityParameter, right, out leftProperty);

        if (!matchedLeft && !matchedRight)
            return false;

        if (!ReferenceEquals(leftProperty, keyProperty))
            return false;

        var valueSide = matchedLeft ? right : left;

        if (ParameterSearchVisitor.Contains(valueSide, entityParameter))
            return false;

        keyType = GetReadTypeForReadField(keyProperty.ClrType);

        // Compile a qc -> keyValue accessor. Use the PK type (or underlying for nullable).
        Expression value = valueSide;
        while (value is UnaryExpression { NodeType: ExpressionType.Convert or ExpressionType.ConvertChecked } uu)
            value = uu.Operand;

        if (value.Type != keyType)
            value = Expression.Convert(value, keyType);

        var boxed = Expression.Convert(value, typeof(object));
        var lambda = Expression.Lambda<Func<QueryContext, object?>>(boxed, queryContextParameter);
        getKeyValue = lambda.Compile();
        return true;

        static bool TryMatchPropertyAccess(IEntityType entityType, ParameterExpression entityParameter, Expression expression, out IProperty? property)
        {
            property = null;

            while (expression is UnaryExpression { NodeType: ExpressionType.Convert or ExpressionType.ConvertChecked } u)
                expression = u.Operand;

            if (expression is MemberExpression { Expression: { } instance, Member: PropertyInfo p } && IsEntityInstance(instance, entityParameter))
            {
                property = entityType.FindProperty(p.Name);
                return property is not null;
            }

            if (expression is MethodCallExpression
                {
                    Method.DeclaringType: var decl,
                    Method.Name: nameof(EF.Property),
                    Arguments: [var inst, ConstantExpression { Value: string s }]
                }
                && decl == typeof(EF)
                && IsEntityInstance(inst, entityParameter))
            {
                property = entityType.FindProperty(s);
                return property is not null;
            }

            return false;

            static bool IsEntityInstance(Expression instance, ParameterExpression entityParameter)
            {
                while (instance is UnaryExpression { NodeType: ExpressionType.Convert or ExpressionType.ConvertChecked } u)
                    instance = u.Operand;

                return instance == entityParameter;
            }
        }
    }
}
