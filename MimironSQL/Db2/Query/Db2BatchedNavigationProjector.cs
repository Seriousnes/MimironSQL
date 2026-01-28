using System.Linq.Expressions;
using System.Reflection;

using MimironSQL.Db2.Model;
using MimironSQL.Db2.Schema;
using MimironSQL.Db2.Wdc5;
using MimironSQL.Formats;

namespace MimironSQL.Db2.Query;

internal static class Db2BatchedNavigationProjector
{
    public static IEnumerable<TResult> Project<TEntity, TResult>(
        IEnumerable<TEntity> source,
        Db2Model model,
        Func<string, (IDb2File File, Db2TableSchema Schema)> tableResolver,
        Db2NavigationMemberAccessPlan[] accesses,
        Expression<Func<TEntity, TResult>> selector,
        int? take)
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

        var lookupByNavigation = new Dictionary<MemberInfo, NavigationLookup>();

        foreach (var (navMember, navAccesses) in accessGroups)
        {
            var join = navAccesses[0].Join;
            var target = model.GetEntityType(join.Navigation.TargetClrType);
            var (relatedFileHandle, relatedSchema) = tableResolver(target.TableName);
            var relatedFile = (Wdc5File)relatedFileHandle;

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

        var rewritten = new SelectorRewriter(selector.Parameters[0], lookupByNavigation, accessGroups).Visit(selector.Body);
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

        var toInt32 = typeof(Convert).GetMethod(nameof(Convert.ToInt32), [typeof(object)])!;
        var call = Expression.Call(toInt32, Expression.Convert(access, typeof(object)));
        return Expression.Lambda<Func<TEntity, int>>(call, entity).Compile();
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

    private sealed class SelectorRewriter(
        ParameterExpression entityParam,
        Dictionary<MemberInfo, NavigationLookup> lookupByNavigation,
        Dictionary<MemberInfo, Db2NavigationMemberAccessPlan[]> accessGroups) : ExpressionVisitor
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
                var memberIndex = lookup.GetMemberIndex(node.Member);

                var tryGet = typeof(NavigationLookup).GetMethod(nameof(NavigationLookup.TryGetValue))!;

                var keyVar = Expression.Variable(typeof(int), "key");
                var valueVar = Expression.Variable(typeof(object), "value");

                var assignKey = Expression.Assign(keyVar, keyGetter);

                var tryGetCall = Expression.Call(Expression.Constant(lookup), tryGet, keyVar, Expression.Constant(memberIndex), valueVar);

                var valueAsTarget = Expression.Convert(valueVar, node.Type);

                var defaultValue = Expression.Default(node.Type);

                var body = Expression.Block(
                    [keyVar, valueVar],
                    assignKey,
                    Expression.Condition(tryGetCall, valueAsTarget, defaultValue));

                return body;
            }

            return node;
        }

        private Expression CreateKeyExpression(MemberInfo member)
        {
            Expression access = member switch
            {
                PropertyInfo p => Expression.Property(entityParam, p),
                FieldInfo f => Expression.Field(entityParam, f),
                _ => throw new InvalidOperationException($"Unexpected member type: {member.GetType().FullName}"),
            };

            var toInt32 = typeof(Convert).GetMethod(nameof(Convert.ToInt32), [typeof(object)])!;
            return Expression.Call(toInt32, Expression.Convert(access, typeof(object)));
        }
    }
}
