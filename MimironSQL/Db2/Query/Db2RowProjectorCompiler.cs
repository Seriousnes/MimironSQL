using MimironSQL.Db2.Schema;

using System.Linq.Expressions;
using System.Reflection;
using MimironSQL.Extensions;
using MimironSQL.Formats;

namespace MimironSQL.Db2.Query;

internal static class Db2RowProjectorCompiler
{
    public static bool TryCompile<TEntity, TResult, TRow>(Db2TableSchema schema, Expression<Func<TEntity, TResult>> selector, out Func<TRow, TResult> projector)
        where TRow : struct
        => TryCompile<TEntity, TResult, TRow>(schema, selector, out projector, out _);

    public static bool TryCompile<TEntity, TResult, TRow>(
        Db2TableSchema schema,
        Expression<Func<TEntity, TResult>> selector,
        out Func<TRow, TResult> projector,
        out Db2SourceRequirements requirements)
        where TRow : struct
    {
        var fieldsByName = schema.Fields.ToDictionary(f => f.Name, StringComparer.OrdinalIgnoreCase);

        requirements = new Db2SourceRequirements(schema, typeof(TEntity));

        var entityParam = selector.Parameters[0];
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
            var rewriter = new SelectorRewriter<TRow>(entityParam, rowParam, GetAccessor, requirements);
            var rewrittenBody = rewriter.Visit(selector.Body);

            if (rewrittenBody is null)
            {
                projector = _ => default!;
                return false;
            }

            projector = Expression.Lambda<Func<TRow, TResult>>(rewrittenBody, rowParam).Compile();
            return true;
        }
        catch (NotSupportedException)
        {
            projector = _ => default!;
            return false;
        }
    }

    private sealed class SelectorRewriter<TRow>(
        ParameterExpression entityParam,
        ParameterExpression rowParam,
        Func<string, Db2FieldAccessor> getAccessor,
        Db2SourceRequirements requirements) : ExpressionVisitor
        where TRow : struct
    {
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
                var read = BuildReadExpression(accessor.Field, m.Type);
                return Expression.Convert(read, node.Type);
            }

            return base.VisitUnary(node);
        }

        protected override Expression VisitBinary(BinaryExpression node)
            => throw new NotSupportedException("Computed selectors are not supported in Phase 3.5.");

        protected override Expression VisitMethodCall(MethodCallExpression node)
            => throw new NotSupportedException("Method calls in selectors are not supported in Phase 3.5.");

        protected override Expression VisitConditional(ConditionalExpression node)
            => throw new NotSupportedException("Conditional expressions in selectors are not supported in Phase 3.5.");

        protected override Expression VisitNewArray(NewArrayExpression node)
            => throw new NotSupportedException("Array creation in selectors is not supported in Phase 3.5.");

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
