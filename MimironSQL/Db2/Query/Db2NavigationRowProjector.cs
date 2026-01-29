using System.Linq.Expressions;
using System.Reflection;

using MimironSQL.Db2.Model;
using MimironSQL.Db2.Schema;
using MimironSQL.Db2.Wdc5;
using MimironSQL.Extensions;

namespace MimironSQL.Db2.Query;

internal static class Db2NavigationRowProjector
{
    public static IEnumerable<TResult> ProjectFromRows<TResult>(
        IEnumerable<Wdc5Row> rows,
        Db2TableSchema rootSchema,
        Db2Model model,
        Func<string, (Wdc5File File, Db2TableSchema Schema)> tableResolver,
        Db2NavigationMemberAccessPlan[] accesses,
        LambdaExpression selector,
        int? take)
    {
        var accessGroups = accesses
            .GroupBy(a => a.Join.Navigation.NavigationMember)
            .ToDictionary(g => g.Key, g => g.ToArray());

        var buffered = new List<Wdc5Row>(take is { } t ? t : 16);
        var keysByNavigation = new Dictionary<MemberInfo, HashSet<int>>();
        var rootKeyAccessorByNavigation = new Dictionary<MemberInfo, Db2FieldAccessor>();

        foreach (var (navMember, navAccesses) in accessGroups)
        {
            keysByNavigation[navMember] = [];
            var keyFieldSchema = navAccesses[0].Join.RootKeyFieldSchema;
            rootKeyAccessorByNavigation[navMember] = new Db2FieldAccessor(keyFieldSchema);
        }

        foreach (var row in rows)
        {
            buffered.Add(row);

            foreach (var (navMember, keys) in keysByNavigation)
            {
                var accessor = rootKeyAccessorByNavigation[navMember];
                var key = Db2RowValue.ReadInt32(row, accessor);
                if (key != 0)
                    keys.Add(key);
            }
        }

        var lookupByNavigation = new Dictionary<MemberInfo, NavigationLookup>();

        foreach (var (navMember, navAccesses) in accessGroups)
        {
            var join = navAccesses[0].Join;
            var target = model.GetEntityType(join.Navigation.TargetClrType);
            var (relatedFile, relatedSchema) = tableResolver(target.TableName);

            var distinctTargetMembers = navAccesses
                .Select(a => a.TargetMember)
                .Distinct()
                .ToArray();

            var accessorByMember = new Dictionary<MemberInfo, Db2FieldAccessor>(capacity: distinctTargetMembers.Length);
            for (var i = 0; i < distinctTargetMembers.Length; i++)
            {
                var member = distinctTargetMembers[i];

                if (!relatedSchema.TryGetField(member.Name, out var field))
                    throw new NotSupportedException($"Field '{member.Name}' was not found in schema '{relatedSchema.TableName}'.");

                accessorByMember[member] = new Db2FieldAccessor(field);
            }

            var keys = keysByNavigation[navMember];
            Dictionary<int, Wdc5Row> rowsByKey = new(capacity: Math.Min(keys.Count, relatedFile.Header.RecordsCount));

            if (keys is { Count: not 0 })
            {
                foreach (var row in relatedFile.EnumerateRows().Where(r => keys.Contains(r.Id)))
                {
                    rowsByKey[row.Id] = row;
                }
            }

            lookupByNavigation[navMember] = new NavigationLookup(rowsByKey, accessorByMember);
        }

        var projectorCompiler = new RowProjectorCompiler(rootSchema, selector, lookupByNavigation, rootKeyAccessorByNavigation);
        var projector = projectorCompiler.Compile<TResult>();

        foreach (var row in buffered)
            yield return projector(row);
    }

    private sealed class NavigationLookup(Dictionary<int, Wdc5Row> rowsByKey, Dictionary<MemberInfo, Db2FieldAccessor> accessorByMember)
    {
        public bool TryGetRow(int key, out Wdc5Row row)
        {
            return rowsByKey.TryGetValue(key, out row);
        }

        public Db2FieldAccessor GetAccessor(MemberInfo member)
            => accessorByMember[member];
    }

    private sealed class RowProjectorCompiler(
        Db2TableSchema rootSchema,
        LambdaExpression selector,
        Dictionary<MemberInfo, NavigationLookup> lookupByNavigation,
        Dictionary<MemberInfo, Db2FieldAccessor> rootKeyAccessorByNavigation)
    {
        private readonly ParameterExpression _entityParam = selector.Parameters[0];
        private readonly ParameterExpression _rowParam = Expression.Parameter(typeof(Wdc5Row), "row");

        public Func<Wdc5Row, TResult> Compile<TResult>()
        {
            var rewriter = new SelectorRewriter(
                _entityParam,
                _rowParam,
                rootSchema,
                lookupByNavigation,
                rootKeyAccessorByNavigation);

            var rewrittenBody = rewriter.Visit(selector.Body);

            if (rewrittenBody is null)
                throw new NotSupportedException("Failed to rewrite selector for row-based projection.");

            return Expression.Lambda<Func<Wdc5Row, TResult>>(rewrittenBody, _rowParam).Compile();
        }

        private sealed class SelectorRewriter(
            ParameterExpression entityParam,
            ParameterExpression rowParam,
            Db2TableSchema rootSchema,
            Dictionary<MemberInfo, NavigationLookup> lookupByNavigation,
            Dictionary<MemberInfo, Db2FieldAccessor> rootKeyAccessorByNavigation) : ExpressionVisitor
        {
            protected override Expression VisitMember(MemberExpression node)
            {
                // x.Nav.Member - handle navigation member access BEFORE visiting base
                if (node.Expression is MemberExpression { Member: PropertyInfo or FieldInfo } nav && nav.Expression == entityParam && lookupByNavigation.TryGetValue(nav.Member, out var lookup))
                {
                    var keyAccessor = rootKeyAccessorByNavigation[nav.Member];
                    var keyExpression = CreateKeyExpression(keyAccessor);
                    var accessor = lookup.GetAccessor(node.Member);

                    var tryGet = typeof(NavigationLookup).GetMethod(nameof(NavigationLookup.TryGetRow))!;

                    var keyVar = Expression.Variable(typeof(int), "key");
                    var relatedRowVar = Expression.Variable(typeof(Wdc5Row), "relatedRow");

                    var assignKey = Expression.Assign(keyVar, keyExpression);
                    var tryGetCall = Expression.Call(Expression.Constant(lookup), tryGet, keyVar, relatedRowVar);

                    var read = BuildReadExpression(relatedRowVar, accessor, node.Type);
                    var defaultValue = Expression.Default(node.Type);

                    return Expression.Block(
                        [keyVar, relatedRowVar],
                        assignKey,
                        Expression.Condition(tryGetCall, read, defaultValue));
                }

                // Visit children first
                node = (MemberExpression)base.VisitMember(node);

                // Direct member access on root entity (x.SomeField)
                if (node.Expression == entityParam)
                {
                    // Check if it's a navigation property (not a field in the schema)
                    if (lookupByNavigation.ContainsKey(node.Member))
                    {
                        // This is a navigation property being accessed directly (x.Nav) without going through a member
                        // We don't support materializing navigation entities in this path, only accessing their fields
                        throw new NotSupportedException(
                            $"Navigation property '{node.Member.Name}' cannot be materialized directly in row-based projections. " +
                            $"Access navigation members explicitly (e.g., x.{node.Member.Name}.SomeField).");
                    }

                    if (!rootSchema.TryGetField(node.Member.Name, out var field))
                    {
                        // Try to find Id field if member name is "Id"
                        if (node.Member.Name.Equals("Id", StringComparison.OrdinalIgnoreCase))
                        {
                            var idField = rootSchema.Fields.FirstOrDefault(f => f.IsId);
                            if (idField.Equals(default(Db2FieldSchema)) || string.IsNullOrWhiteSpace(idField.Name))
                                throw new NotSupportedException($"Id field was not found in schema '{rootSchema.TableName}'.");
                            field = idField;
                        }
                        else
                        {
                            throw new NotSupportedException($"Field '{node.Member.Name}' was not found in schema '{rootSchema.TableName}'.");
                        }
                    }

                    var accessor = new Db2FieldAccessor(field);
                    return BuildReadExpression(accessor, node.Type);
                }

                return node;
            }

            private Expression CreateKeyExpression(Db2FieldAccessor accessor)
            {
                var readInt32 = typeof(int).GetReadMethod();
                return Expression.Call(readInt32, rowParam, Expression.Constant(accessor));
            }

            private Expression BuildReadExpression(Db2FieldAccessor accessor, Type targetType)
                => BuildReadExpression(rowParam, accessor, targetType);

            private static Expression BuildReadExpression(Expression rowExpression, Db2FieldAccessor accessor, Type targetType)
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
                    return Expression.Call(generic, rowExpression, Expression.Constant(accessor));
                }

                if (targetType == typeof(string) && accessor.Field.IsVirtual)
                    throw new NotSupportedException($"Virtual field '{accessor.Field.Name}' cannot be materialized as a string.");

                var readType = targetType.UnwrapNullable();
                var methodInfo = readType.GetReadMethod();
                var read = Expression.Call(methodInfo, rowExpression, Expression.Constant(accessor));
                return targetType.IsNullable() ? Expression.Convert(read, targetType) : read;
            }
        }
    }
}
