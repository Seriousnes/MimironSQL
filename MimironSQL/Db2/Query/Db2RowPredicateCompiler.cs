using MimironSQL.Db2.Schema;
using MimironSQL.Extensions;
using MimironSQL.Formats;

using System.Linq.Expressions;
using System.Reflection;

namespace MimironSQL.Db2.Query;

internal static class Db2RowPredicateCompiler
{
    public static bool TryCompile<TEntity, TRow>(IDb2File<TRow> file, Db2TableSchema schema, Expression<Func<TEntity, bool>> predicate, out Func<TRow, bool> rowPredicate)
        where TRow : struct
        => TryCompile(file, schema, predicate, out rowPredicate, out _);

    public static bool TryCompile<TEntity, TRow>(
        IDb2File<TRow> file,
        Db2TableSchema schema,
        Expression<Func<TEntity, bool>> predicate,
        out Func<TRow, bool> rowPredicate,
        out Db2SourceRequirements requirements)
        where TRow : struct
    {
        var fieldsByName = schema.Fields.ToDictionary(f => f.Name, StringComparer.OrdinalIgnoreCase);

        requirements = new Db2SourceRequirements(schema, typeof(TEntity));

        var entityParam = predicate.Parameters[0];
        var rowParam = Expression.Parameter(typeof(TRow), "row");

        var accessors = new Dictionary<string, Db2FieldAccessor>(StringComparer.OrdinalIgnoreCase);

        Db2FieldAccessor GetAccessor(string name)
        {
            if (accessors.TryGetValue(name, out var existing))
                return existing;

            if (!fieldsByName.TryGetValue(name, out var field))
                throw new NotSupportedException($"Field '{name}' was not found in schema '{schema.TableName}'.");

            var created = new Db2FieldAccessor(field);
            accessors[name] = created;
            return created;
        }

        try
        {
            var rewriter = new PredicateRewriter<TRow>(entityParam, rowParam, GetAccessor, file, requirements);
            var rewrittenBody = rewriter.Visit(predicate.Body);
            if (rewrittenBody is null)
            {
                rowPredicate = _ => false;
                return false;
            }

            rowPredicate = Expression.Lambda<Func<TRow, bool>>(rewrittenBody, rowParam).Compile();
            return true;
        }
        catch (NotSupportedException)
        {
            rowPredicate = _ => false;
            return false;
        }
    }

    private sealed class PredicateRewriter<TRow>(
        ParameterExpression entityParam,
        ParameterExpression rowParam,
        Func<string, Db2FieldAccessor> getAccessor,
        IDb2File<TRow> file,
        Db2SourceRequirements requirements) : ExpressionVisitor
        where TRow : struct
    {
        private readonly IDb2DenseStringTableIndexProvider<TRow>? _denseIndexProvider = file as IDb2DenseStringTableIndexProvider<TRow>;

        protected override Expression VisitParameter(ParameterExpression node)
        {
            if (node == entityParam)
                throw new NotSupportedException("Unexpected naked entity parameter.");

            return base.VisitParameter(node);
        }

        protected override Expression VisitMember(MemberExpression node)
        {
            if (node.Expression == entityParam)
            {
                var accessor = getAccessor(node.Member.Name);
                requirements.RequireField(accessor.Field, node.Type == typeof(string) ? Db2RequiredColumnKind.String : Db2RequiredColumnKind.Scalar);
                if (node.Type == typeof(string) && accessor.Field.IsVirtual)
                    throw new NotSupportedException($"Virtual field '{accessor.Field.Name}' cannot be materialized as a string.");
                return BuildReadExpression(accessor.Field, node.Type);
            }

            return base.VisitMember(node);
        }

        protected override Expression VisitUnary(UnaryExpression node)
        {
            if (node is { NodeType: ExpressionType.Convert } && node.Operand is MemberExpression m && m.Expression == entityParam)
            {
                var accessor = getAccessor(m.Member.Name);
                requirements.RequireField(accessor.Field, m.Type == typeof(string) ? Db2RequiredColumnKind.String : Db2RequiredColumnKind.Scalar);
                if (m.Type == typeof(string) && accessor.Field.IsVirtual)
                    throw new NotSupportedException($"Virtual field '{accessor.Field.Name}' cannot be materialized as a string.");
                var read = BuildReadExpression(accessor.Field, m.Type);
                return Expression.Convert(read, node.Type);
            }

            return base.VisitUnary(node);
        }

        protected override Expression VisitMethodCall(MethodCallExpression node)
        {
            if (node.Method.DeclaringType == typeof(string) && node.Object is MemberExpression m && m.Expression == entityParam)
            {
                var methodName = node.Method.Name;
                if (methodName is nameof(string.Contains) or nameof(string.StartsWith) or nameof(string.EndsWith))
                {
                    if (node.Arguments is not { Count: 1 } || node.Arguments[0] is not ConstantExpression { Value: string needle })
                        throw new NotSupportedException("Only constant string needles are supported for string predicates.");

                    var accessor = getAccessor(m.Member.Name);
                    requirements.RequireField(accessor.Field, Db2RequiredColumnKind.String);

                    if (accessor.Field.IsVirtual)
                        throw new NotSupportedException($"Virtual field '{accessor.Field.Name}' cannot be materialized as a string.");

                    var isDenseOptimizable = _denseIndexProvider is not null && !file.Flags.HasFlag(Db2Flags.Sparse) && !accessor.Field.IsVirtual;
                    if (isDenseOptimizable)
                    {
                        var kind = methodName switch
                        {
                            nameof(string.Contains) => Db2StringMatchKind.Contains,
                            nameof(string.StartsWith) => Db2StringMatchKind.StartsWith,
                            nameof(string.EndsWith) => Db2StringMatchKind.EndsWith,
                            _ => Db2StringMatchKind.Contains,
                        };

                        var starts = Db2DenseStringScanner.FindStartOffsets(file.DenseStringTableBytes.Span, needle, kind);
                        var matchMethod = typeof(Db2DenseStringMatch)
                            .GetMethod(methodName, BindingFlags.Public | BindingFlags.Static)!
                            .MakeGenericMethod(typeof(TRow));

                        return Expression.Call(
                            matchMethod,
                            Expression.Constant(_denseIndexProvider),
                            rowParam,
                            Expression.Constant(accessor.Field.ColumnStartIndex),
                            Expression.Constant(starts));
                    }

                    var instance = BuildReadExpression(accessor.Field, typeof(string));
                    var visitedArgs = node.Arguments.Select(a => (Expression)Visit(a)!);
                    return Expression.Call(instance, node.Method, visitedArgs);
                }
            }

            return base.VisitMethodCall(node);
        }

        private Expression BuildReadExpression(Db2FieldSchema field, Type targetType)
        {
            if (TryMapSchemaArrayCollectionRead(field, targetType, out var readType))
            {
                var getArray = typeof(TRow)
                    .GetMethod(nameof(IDb2Row.Get), BindingFlags.Instance | BindingFlags.Public, [typeof(int)])!
                    .MakeGenericMethod(readType);

                var readArray = Expression.Call(rowParam, getArray, Expression.Constant(field.ColumnStartIndex));
                return Expression.Convert(readArray, targetType);
            }

            var get = typeof(TRow)
                .GetMethod(nameof(IDb2Row.Get), BindingFlags.Instance | BindingFlags.Public, [typeof(int)])!
                .MakeGenericMethod(targetType);

            return Expression.Call(rowParam, get, Expression.Constant(field.ColumnStartIndex));
        }

        private static bool TryMapSchemaArrayCollectionRead(Db2FieldSchema field, Type targetType, out Type readType)
        {
            if (field.ElementCount <= 1)
            {
                readType = null!;
                return false;
            }

            if (!targetType.IsGenericType)
            {
                readType = null!;
                return false;
            }

            var genericDefinition = targetType.GetGenericTypeDefinition();
            if (genericDefinition != typeof(ICollection<>)
                && genericDefinition != typeof(IList<>)
                && genericDefinition != typeof(IEnumerable<>)
                && genericDefinition != typeof(IReadOnlyCollection<>)
                && genericDefinition != typeof(IReadOnlyList<>))
            {
                readType = null!;
                return false;
            }

            var elementType = targetType.GetGenericArguments()[0];
            if (elementType == typeof(string))
            {
                readType = null!;
                return false;
            }

            if (!elementType.IsPrimitive && elementType != typeof(float) && elementType != typeof(double))
            {
                readType = null!;
                return false;
            }

            readType = elementType.MakeArrayType();
            return true;
        }
    }
}
