using System.Linq.Expressions;
using System.Reflection;

using MimironSQL.Db2.Model;
using MimironSQL.Db2.Schema;
using MimironSQL.Formats;
using MimironSQL.Extensions;

namespace MimironSQL.Db2.Query;

internal static class Db2BatchedNavigationProjector
{
    public static IEnumerable<TResult> Project<TEntity, TResult, TRow>(
        IEnumerable<TEntity> source,
        Db2Model model,
        Func<string, (IDb2File<TRow> File, Db2TableSchema Schema)> tableResolver,
        Db2NavigationMemberAccessPlan[] accesses,
        Expression<Func<TEntity, TResult>> selector,
        int? take)
        where TRow : struct, IDb2Row
    {
        var accessGroups = accesses
            .GroupBy(a => a.Join.Navigation.NavigationMember)
            .ToDictionary(g => g.Key, g => g.ToArray());

        var buffered = new List<TEntity>(take is { } t ? t : 16);
        var keysByNavigation = new Dictionary<MemberInfo, HashSet<int>>();
        var keyGetterByNavigation = new Dictionary<MemberInfo, Func<TEntity, int>>();

        foreach (var (navMember, navAccesses) in accessGroups)
        {
            keysByNavigation[navMember] = [];
            keyGetterByNavigation[navMember] = CreateIntGetter<TEntity>(navAccesses[0].Join.RootKeyMember);
        }

        foreach (var entity in source)
        {
            buffered.Add(entity);

            foreach (var (navMember, keys) in keysByNavigation)
            {
                var key = keyGetterByNavigation[navMember](entity);
                if (key != 0)
                    keys.Add(key);
            }

            if (take.HasValue && buffered.Count >= take.Value)
                break;
        }

        var lookupByNavigation = new Dictionary<MemberInfo, NavigationLookup<TRow>>();

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
            Dictionary<int, TRow> rowsByKey = new(capacity: Math.Min(keys.Count, relatedFile.RecordsCount));

            if (keys is { Count: not 0 })
            {
                foreach (var row in relatedFile.EnumerateRows())
                {
                    var rowId = row.Get<int>(Db2VirtualFieldIndex.Id);
                    if (!keys.Contains(rowId))
                        continue;

                    rowsByKey[rowId] = row;
                }
            }

            lookupByNavigation[navMember] = new NavigationLookup<TRow>(rowsByKey, accessorByMember);
        }

        var rewritten = new SelectorRewriter<TRow>(selector.Parameters[0], lookupByNavigation, accessGroups).Visit(selector.Body);
        var projection = Expression.Lambda<Func<TEntity, TResult>>(rewritten!, selector.Parameters[0]).Compile();

        foreach (var entity in buffered)
            yield return projection(entity);
    }

    private static Func<TEntity, int> CreateIntGetter<TEntity>(MemberInfo member)
    {
        var entity = Expression.Parameter(typeof(TEntity), "entity");

        Expression access = member switch
        {
            PropertyInfo p => Expression.Property(entity, p),
            FieldInfo f => Expression.Field(entity, f),
            _ => throw new InvalidOperationException($"Unexpected member type: {member.GetType().FullName}"),
        };

        var memberType = access.Type;
        Expression key = memberType == typeof(int)
            ? access
            : memberType == typeof(long)
                ? Expression.Call(typeof(Convert).GetMethod(nameof(Convert.ToInt32), [typeof(long)])!, access)
                : memberType == typeof(uint)
                    ? Expression.Call(typeof(Convert).GetMethod(nameof(Convert.ToInt32), [typeof(uint)])!, access)
                    : memberType == typeof(ulong)
                        ? Expression.Call(typeof(Convert).GetMethod(nameof(Convert.ToInt32), [typeof(ulong)])!, access)
                        : memberType == typeof(short)
                            ? Expression.Convert(access, typeof(int))
                            : memberType == typeof(ushort)
                                ? Expression.Convert(access, typeof(int))
                                : memberType == typeof(byte)
                                    ? Expression.Convert(access, typeof(int))
                                    : memberType == typeof(sbyte)
                                        ? Expression.Convert(access, typeof(int))
                                        : memberType.IsEnum
                                            ? Expression.Convert(Expression.Convert(access, Enum.GetUnderlyingType(memberType)), typeof(int))
                                            : throw new NotSupportedException($"Unsupported key member type {memberType.FullName}.");

        return Expression.Lambda<Func<TEntity, int>>(key, entity).Compile();
    }

    private sealed class NavigationLookup<TRow>(Dictionary<int, TRow> rowsByKey, Dictionary<MemberInfo, Db2FieldAccessor> accessorByMember)
        where TRow : struct, IDb2Row
    {
        public bool TryGetRow(int key, out TRow row)
        {
            return rowsByKey.TryGetValue(key, out row);
        }

        public Db2FieldAccessor GetAccessor(MemberInfo member)
            => accessorByMember[member];
    }

    private sealed class SelectorRewriter<TRow>(
        ParameterExpression entityParam,
        Dictionary<MemberInfo, NavigationLookup<TRow>> lookupByNavigation,
        Dictionary<MemberInfo, Db2NavigationMemberAccessPlan[]> accessGroups) : ExpressionVisitor
        where TRow : struct, IDb2Row
    {
        protected override Expression VisitMember(MemberExpression node)
        {
            node = (MemberExpression)base.VisitMember(node);

            // x.Nav.Member
            if (node.Expression is MemberExpression { Member: PropertyInfo or FieldInfo } nav && nav.Expression == entityParam)
            {
                if (!lookupByNavigation.TryGetValue(nav.Member, out var lookup))
                    return node;

                var plans = accessGroups[nav.Member];
                var join = plans[0].Join;

                var keyGetter = CreateKeyExpression(join.RootKeyMember);
                var accessor = lookup.GetAccessor(node.Member);

                var tryGet = typeof(NavigationLookup<TRow>).GetMethod(nameof(NavigationLookup<TRow>.TryGetRow))!;

                var keyVar = Expression.Variable(typeof(int), "key");
                var relatedRowVar = Expression.Variable(typeof(TRow), "relatedRow");

                var assignKey = Expression.Assign(keyVar, keyGetter);
                var tryGetCall = Expression.Call(Expression.Constant(lookup), tryGet, keyVar, relatedRowVar);

                var read = BuildReadExpression(relatedRowVar, accessor, node.Type);
                var defaultValue = Expression.Default(node.Type);

                return Expression.Block(
                    [keyVar, relatedRowVar],
                    assignKey,
                    Expression.Condition(tryGetCall, read, defaultValue));
            }

            return node;
        }

        private Expression CreateKeyExpression(MemberInfo member)
        {
            return member.CreateInt32KeyExpression(entityParam);
        }

        private static Expression BuildReadExpression(Expression rowExpression, Db2FieldAccessor accessor, Type targetType)
        {
            if (accessor.Field.ElementCount > 1
                && targetType.IsGenericType
                && (targetType.GetGenericTypeDefinition() == typeof(ICollection<>)
                    || targetType.GetGenericTypeDefinition() == typeof(IList<>)
                    || targetType.GetGenericTypeDefinition() == typeof(IEnumerable<>)
                    || targetType.GetGenericTypeDefinition() == typeof(IReadOnlyCollection<>)
                    || targetType.GetGenericTypeDefinition() == typeof(IReadOnlyList<>)))
            {
                var elementType = targetType.GetGenericArguments()[0];
                if (elementType == typeof(string))
                    throw new NotSupportedException("String arrays are not supported.");

                if (!elementType.IsPrimitive && elementType != typeof(float) && elementType != typeof(double))
                    throw new NotSupportedException($"Unsupported array element type {elementType.FullName}.");

                var arrayType = elementType.MakeArrayType();
                var getArray = typeof(TRow).GetMethod(nameof(IDb2Row.Get), BindingFlags.Public | BindingFlags.Instance)!
                    .MakeGenericMethod(arrayType);

                var readArray = Expression.Call(rowExpression, getArray, Expression.Constant(accessor.Field.ColumnStartIndex));
                return Expression.Convert(readArray, targetType);
            }

            if (targetType.IsArray)
            {
                var elementType = targetType.GetElementType()!;
                if (elementType == typeof(string))
                    throw new NotSupportedException("String arrays are not supported.");

                if (!elementType.IsPrimitive && elementType != typeof(float) && elementType != typeof(double))
                    throw new NotSupportedException($"Unsupported array element type {elementType.FullName}.");
            }

            if (targetType == typeof(string) && accessor.Field.IsVirtual)
                throw new NotSupportedException($"Virtual field '{accessor.Field.Name}' cannot be materialized as a string.");

            var readType = targetType.UnwrapNullable();
            var methodInfo = typeof(TRow).GetMethod(nameof(IDb2Row.Get), BindingFlags.Public | BindingFlags.Instance)!
                .MakeGenericMethod(readType);

            var read = Expression.Call(
                rowExpression,
                methodInfo,
                Expression.Constant(accessor.Field.ColumnStartIndex));

            return targetType.IsNullable() ? Expression.Convert(read, targetType) : read;
        }        
    }
}
