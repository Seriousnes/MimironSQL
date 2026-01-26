using MimironSQL.Db2.Schema;
using MimironSQL.Db2.Wdc5;

using System.Linq.Expressions;
using System.Reflection;

namespace MimironSQL.Db2.Query;

internal static class Db2RowProjectorCompiler
{
    public static bool TryCompile<TEntity, TResult>(Db2TableSchema schema, Expression<Func<TEntity, TResult>> selector, out Func<Wdc5Row, TResult> projector)
        => TryCompile(schema, selector, out projector, out _);

    public static bool TryCompile<TEntity, TResult>(
        Db2TableSchema schema,
        Expression<Func<TEntity, TResult>> selector,
        out Func<Wdc5Row, TResult> projector,
        out Db2SourceRequirements requirements)
    {
        var fieldsByName = schema.Fields.ToDictionary(f => f.Name, StringComparer.OrdinalIgnoreCase);

        requirements = new Db2SourceRequirements(schema, typeof(TEntity));

        var entityParam = selector.Parameters[0];
        var rowParam = Expression.Parameter(typeof(Wdc5Row), "row");

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
            var rewriter = new SelectorRewriter(entityParam, rowParam, GetAccessor, requirements);
            var rewrittenBody = rewriter.Visit(selector.Body);

            if (rewrittenBody is null)
            {
                projector = _ => default!;
                return false;
            }

            projector = Expression.Lambda<Func<Wdc5Row, TResult>>(rewrittenBody, rowParam).Compile();
            return true;
        }
        catch (NotSupportedException)
        {
            projector = _ => default!;
            return false;
        }
    }

    private sealed class SelectorRewriter(
        ParameterExpression entityParam,
        ParameterExpression rowParam,
        Func<string, Db2FieldAccessor> getAccessor,
        Db2SourceRequirements requirements) : ExpressionVisitor
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
                return BuildReadExpression(accessor, node.Type);
            }

            return base.VisitMember(node);
        }

        protected override Expression VisitUnary(UnaryExpression node)
        {
            if (node.NodeType == ExpressionType.Convert && node.Operand is MemberExpression m && m.Expression == entityParam)
            {
                var accessor = getAccessor(m.Member.Name);
                requirements.RequireField(accessor.Field, m.Type == typeof(string) ? Db2RequiredColumnKind.String : Db2RequiredColumnKind.Scalar);
                var read = BuildReadExpression(accessor, m.Type);
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

        private static bool IsNullable(Type t)
            => t.IsGenericType && t.GetGenericTypeDefinition() == typeof(Nullable<>);

        private static Type UnwrapNullable(Type t)
            => IsNullable(t) ? Nullable.GetUnderlyingType(t)! : t;

        private Expression BuildReadExpression(Db2FieldAccessor accessor, Type targetType)
        {
            if (targetType.IsArray)
            {
                var elementType = targetType.GetElementType()!;
                if (elementType == typeof(string))
                    throw new NotSupportedException("String arrays are not supported.");

                if (!elementType.IsPrimitive && elementType != typeof(float) && elementType != typeof(double))
                    throw new NotSupportedException($"Unsupported array element type {elementType.FullName}.");

                var method = typeof(Db2RowValue).GetMethod(nameof(Db2RowValue.ReadArray), BindingFlags.Public | BindingFlags.Static)!;
                var generic = method.MakeGenericMethod(elementType);
                return Expression.Call(generic, rowParam, Expression.Constant(accessor));
            }

            if (targetType == typeof(string) && accessor.Field.IsVirtual)
                throw new NotSupportedException($"Virtual field '{accessor.Field.Name}' cannot be materialized as a string.");

            var methodInfo = typeof(Db2RowValue).GetMethod(nameof(Db2RowValue.Read), BindingFlags.Public | BindingFlags.Static)!;
            var readType = UnwrapNullable(targetType);
            var genericRead = methodInfo.MakeGenericMethod(readType);
            var read = Expression.Call(genericRead, rowParam, Expression.Constant(accessor));

            if (IsNullable(targetType))
                return Expression.Convert(read, targetType);

            return read;
        }
    }
}
