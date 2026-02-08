using System.Linq.Expressions;
using System.Reflection;

using MimironSQL.Formats;
using MimironSQL.EntityFrameworkCore.Db2.Model;
using MimironSQL.EntityFrameworkCore.Db2.Schema;

namespace MimironSQL.EntityFrameworkCore.Db2.Query;

internal static class Db2RowProjectorCompiler
{
    public static bool TryCompile<TEntity, TResult, TRow>(Db2EntityType entityType, Expression<Func<TEntity, TResult>> selector, out Func<TRow, TResult> projector)
        where TRow : struct, IRowHandle
        => TryCompile<TEntity, TResult, TRow>(file: null, entityType, selector, out projector, out _);

    public static bool TryCompile<TEntity, TResult, TRow>(
        IDb2File? file,
        Db2EntityType entityType,
        Expression<Func<TEntity, TResult>> selector,
        out Func<TRow, TResult> projector,
        out Db2SourceRequirements requirements)
        where TRow : struct, IRowHandle
    {
        var schema = entityType.Schema;
        var fieldsByName = schema.Fields.ToDictionary(f => f.Name, StringComparer.OrdinalIgnoreCase);

        requirements = new Db2SourceRequirements(entityType);

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
            var rewriter = new SelectorRewriter<TRow>(entityParam, rowParam, entityType, GetAccessor, requirements, file);
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
        Db2EntityType entityType,
        Func<string, Db2FieldAccessor> getAccessor,
        Db2SourceRequirements requirements,
        IDb2File? file) : ExpressionVisitor
        where TRow : struct, IRowHandle
    {
        private readonly ConstantExpression? _fileExpression = file is null ? null : Expression.Constant(file, typeof(IDb2File));
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
                if (node.Member is not PropertyInfo)
                    throw new NotSupportedException($"Member '{entityType.ClrType.FullName}.{node.Member.Name}' must be a public property.");

                var field = entityType.ResolveFieldSchema(node.Member, context: "row projection");
                var accessor = getAccessor(field.Name);
                requirements.RequireMember(node.Member, node.Type == typeof(string) ? Db2RequiredColumnKind.String : Db2RequiredColumnKind.Scalar);
                return BuildReadExpression(accessor.Field, node.Type);
            }

            return base.VisitMember(node);
        }

        protected override Expression VisitUnary(UnaryExpression node)
        {
            switch (node)
            {
                case { NodeType: ExpressionType.Convert } when node.Operand is MemberExpression m && m.Expression == entityParam:
                    {
                        if (m.Member is not PropertyInfo)
                            throw new NotSupportedException($"Member '{entityType.ClrType.FullName}.{m.Member.Name}' must be a public property.");

                        var field = entityType.ResolveFieldSchema(m.Member, context: "row projection");
                        var accessor = getAccessor(field.Name);
                        requirements.RequireMember(m.Member, m.Type == typeof(string) ? Db2RequiredColumnKind.String : Db2RequiredColumnKind.Scalar);
                        var read = BuildReadExpression(accessor.Field, m.Type);
                        return Expression.Convert(read, node.Type);
                    }

                default:
                    return base.VisitUnary(node);
            }
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
            if (_fileExpression is null)
                throw new NotSupportedException("Row-level projection requires an IDb2File instance.");

            if (TryMapSchemaArrayCollectionRead(field, targetType, out var readType))
            {
                var readArrayMethod = typeof(Db2RowHandleAccess)
                    .GetMethod(nameof(Db2RowHandleAccess.ReadField), BindingFlags.Public | BindingFlags.Static)!
                    .MakeGenericMethod(typeof(TRow), readType);

                var readArray = Expression.Call(readArrayMethod, _fileExpression, rowParam, Expression.Constant(field.ColumnStartIndex));
                return Expression.Convert(readArray, targetType);
            }

            var readMethod = typeof(Db2RowHandleAccess)
                .GetMethod(nameof(Db2RowHandleAccess.ReadField), BindingFlags.Public | BindingFlags.Static)!
                .MakeGenericMethod(typeof(TRow), targetType);

            return Expression.Call(readMethod, _fileExpression, rowParam, Expression.Constant(field.ColumnStartIndex));
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
