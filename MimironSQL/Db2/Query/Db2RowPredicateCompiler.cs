using MimironSQL.Db2.Schema;
using MimironSQL.Db2.Wdc5;

using System.Linq.Expressions;
using System.Reflection;

namespace MimironSQL.Db2.Query;

internal static class Db2RowPredicateCompiler
{
    public static bool TryCompile<TEntity>(Wdc5File file, Db2TableSchema schema, Expression<Func<TEntity, bool>> predicate, out Func<Wdc5Row, bool> rowPredicate)
    {
        var fieldsByName = schema.Fields.ToDictionary(f => f.Name, StringComparer.OrdinalIgnoreCase);

        var entityParam = predicate.Parameters[0];
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
            var rewriter = new PredicateRewriter(entityParam, rowParam, GetAccessor, file);
            var rewrittenBody = rewriter.Visit(predicate.Body);
            if (rewrittenBody is null)
            {
                rowPredicate = _ => false;
                return false;
            }

            rowPredicate = Expression.Lambda<Func<Wdc5Row, bool>>(rewrittenBody, rowParam).Compile();
            return true;
        }
        catch (NotSupportedException)
        {
            rowPredicate = _ => false;
            return false;
        }
    }

    private sealed class PredicateRewriter(
        ParameterExpression entityParam,
        ParameterExpression rowParam,
        Func<string, Db2FieldAccessor> getAccessor,
        Wdc5File file) : ExpressionVisitor
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
                return BuildReadExpression(accessor, node.Type);
            }

            return base.VisitMember(node);
        }

        protected override Expression VisitUnary(UnaryExpression node)
        {
            if (node.NodeType == ExpressionType.Convert && node.Operand is MemberExpression m && m.Expression == entityParam)
            {
                var accessor = getAccessor(m.Member.Name);
                var read = BuildReadExpression(accessor, m.Type);
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
                    if (node.Arguments.Count != 1 || node.Arguments[0] is not ConstantExpression { Value: string needle })
                        throw new NotSupportedException("Only constant string needles are supported for string predicates.");

                    var accessor = getAccessor(m.Member.Name);

                    var isDenseOptimizable = !file.Header.Flags.HasFlag(Db2.Db2Flags.Sparse) && !accessor.Field.IsVirtual;
                    if (isDenseOptimizable)
                    {
                        var kind = methodName switch
                        {
                            nameof(string.Contains) => Db2StringMatchKind.Contains,
                            nameof(string.StartsWith) => Db2StringMatchKind.StartsWith,
                            nameof(string.EndsWith) => Db2StringMatchKind.EndsWith,
                            _ => Db2StringMatchKind.Contains,
                        };

                        var starts = Db2DenseStringScanner.FindStartOffsets(file.DenseStringTableBytes, needle, kind);
                        var matchMethod = typeof(Db2DenseStringMatch).GetMethod(methodName, BindingFlags.Public | BindingFlags.Static);
                        return Expression.Call(matchMethod!, rowParam, Expression.Constant(accessor), Expression.Constant(starts));
                    }

                    var instance = BuildReadExpression(accessor, typeof(string));
                    var visitedArgs = node.Arguments.Select(a => (Expression)Visit(a)!);
                    return Expression.Call(instance, node.Method, visitedArgs);
                }
            }

            return base.VisitMethodCall(node);
        }

        private Expression BuildReadExpression(Db2FieldAccessor accessor, Type targetType)
        {
            var method = typeof(Db2RowValue).GetMethod(nameof(Db2RowValue.Read), BindingFlags.Public | BindingFlags.Static)!;
            var generic = method.MakeGenericMethod(targetType);
            return Expression.Call(generic, rowParam, Expression.Constant(accessor));
        }
    }
}
