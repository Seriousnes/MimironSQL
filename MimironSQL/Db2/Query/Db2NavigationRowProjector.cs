using System.Linq.Expressions;
using System.Reflection;

using MimironSQL.Db2.Model;
using MimironSQL.Db2.Schema;
using MimironSQL.Db2.Wdc5;

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
                var key = ReadInt32(row, accessor);
                if (key != 0)
                    keys.Add(key);
            }

            if (take.HasValue && buffered.Count >= take.Value)
                break;
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

            var memberIndexes = new Dictionary<MemberInfo, int>();
            for (var i = 0; i < distinctTargetMembers.Length; i++)
                memberIndexes[distinctTargetMembers[i]] = i;

            var readers = new Func<Wdc5Row, object?>[distinctTargetMembers.Length];
            for (var i = 0; i < distinctTargetMembers.Length; i++)
            {
                var member = distinctTargetMembers[i];

                if (!relatedSchema.TryGetField(member.Name, out var field))
                    throw new NotSupportedException($"Field '{member.Name}' was not found in schema '{relatedSchema.TableName}'.");

                var accessor = new Db2FieldAccessor(field);
                readers[i] = CreateFieldReader(member, accessor);
            }

            var keys = keysByNavigation[navMember];
            Dictionary<int, object?[]> valuesByKey = new(capacity: Math.Min(keys.Count, relatedFile.Header.RecordsCount));

            if (keys is { Count: not 0 })
            {
                foreach (var row in relatedFile.EnumerateRows())
                {
                    if (!keys.Contains(row.Id))
                        continue;

                    var values = new object?[readers.Length];
                    for (var i = 0; i < readers.Length; i++)
                        values[i] = readers[i](row);

                    valuesByKey[row.Id] = values;
                }
            }

            lookupByNavigation[navMember] = new NavigationLookup(valuesByKey, memberIndexes);
        }

        var projectorCompiler = new RowProjectorCompiler(rootSchema, selector, lookupByNavigation, accessGroups, rootKeyAccessorByNavigation);
        var projector = projectorCompiler.Compile<TResult>();

        foreach (var row in buffered)
            yield return projector(row);
    }

    private static int ReadInt32(Wdc5Row row, Db2FieldAccessor accessor)
    {
        if (accessor.Field.IsVirtual)
            return row.Id;

        var index = accessor.Field.ColumnStartIndex;
        return Convert.ToInt32(row.GetScalar<long>(index));
    }

    private static Func<Wdc5Row, object?> CreateFieldReader(MemberInfo member, Db2FieldAccessor accessor)
    {
        var memberType = member switch
        {
            PropertyInfo p => p.PropertyType,
            FieldInfo f => f.FieldType,
            _ => throw new InvalidOperationException($"Unexpected member type: {member.GetType().FullName}"),
        };

        if (memberType == typeof(string) && accessor.Field.IsVirtual)
            throw new NotSupportedException($"Virtual field '{accessor.Field.Name}' cannot be materialized as a string.");

        var row = Expression.Parameter(typeof(Wdc5Row), "row");

        var read = typeof(Db2RowValue)
            .GetMethod(nameof(Db2RowValue.Read), BindingFlags.Public | BindingFlags.Static)!
            .MakeGenericMethod(memberType);

        var call = Expression.Call(read, row, Expression.Constant(accessor));
        var boxed = Expression.Convert(call, typeof(object));

        return Expression.Lambda<Func<Wdc5Row, object?>>(boxed, row).Compile();
    }

    private sealed class NavigationLookup(Dictionary<int, object?[]> valuesByKey, Dictionary<MemberInfo, int> memberIndexes)
    {
        public bool TryGetValue(int key, int memberIndex, out object? value)
        {
            if (valuesByKey.TryGetValue(key, out var arr) && (uint)memberIndex < (uint)arr.Length)
            {
                value = arr[memberIndex];
                return true;
            }

            value = null;
            return false;
        }

        public int GetMemberIndex(MemberInfo member)
            => memberIndexes[member];
    }

    private sealed class RowProjectorCompiler(
        Db2TableSchema rootSchema,
        LambdaExpression selector,
        Dictionary<MemberInfo, NavigationLookup> lookupByNavigation,
        Dictionary<MemberInfo, Db2NavigationMemberAccessPlan[]> accessGroups,
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
                accessGroups,
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
            Dictionary<MemberInfo, Db2NavigationMemberAccessPlan[]> accessGroups,
            Dictionary<MemberInfo, Db2FieldAccessor> rootKeyAccessorByNavigation) : ExpressionVisitor
        {
            protected override Expression VisitMember(MemberExpression node)
            {
                // x.Nav.Member - handle navigation member access BEFORE visiting base
                if (node.Expression is MemberExpression { Member: PropertyInfo or FieldInfo } nav && nav.Expression == entityParam)
                {
                    if (lookupByNavigation.TryGetValue(nav.Member, out var lookup))
                    {
                        var plans = accessGroups[nav.Member];
                        var join = plans[0].Join;

                        var keyAccessor = rootKeyAccessorByNavigation[nav.Member];
                        var keyExpression = CreateKeyExpression(keyAccessor);
                        var memberIndex = lookup.GetMemberIndex(node.Member);

                        var tryGet = typeof(NavigationLookup).GetMethod(nameof(NavigationLookup.TryGetValue))!;

                        var keyVar = Expression.Variable(typeof(int), "key");
                        var valueVar = Expression.Variable(typeof(object), "value");

                        var assignKey = Expression.Assign(keyVar, keyExpression);

                        var tryGetCall = Expression.Call(Expression.Constant(lookup), tryGet, keyVar, Expression.Constant(memberIndex), valueVar);

                        var valueAsTarget = Expression.Convert(valueVar, node.Type);

                        var defaultValue = Expression.Default(node.Type);

                        var body = Expression.Block(
                            [keyVar, valueVar],
                            assignKey,
                            Expression.Condition(tryGetCall, valueAsTarget, defaultValue));

                        return body;
                    }
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
                if (accessor.Field.IsVirtual)
                {
                    var idProperty = typeof(Wdc5Row).GetProperty(nameof(Wdc5Row.Id))!;
                    return Expression.Property(rowParam, idProperty);
                }

                var index = accessor.Field.ColumnStartIndex;
                var getScalar = typeof(Wdc5Row).GetMethod(nameof(Wdc5Row.GetScalar))!.MakeGenericMethod(typeof(long));
                var scalar = Expression.Call(rowParam, getScalar, Expression.Constant(index));
                var toInt32 = typeof(Convert).GetMethod(nameof(Convert.ToInt32), [typeof(long)])!;
                return Expression.Call(toInt32, scalar);
            }

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

            private static bool IsNullable(Type t)
                => t.IsGenericType && t.GetGenericTypeDefinition() == typeof(Nullable<>);

            private static Type UnwrapNullable(Type t)
                => IsNullable(t) ? Nullable.GetUnderlyingType(t)! : t;
        }
    }
}
