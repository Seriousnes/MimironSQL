using System.Linq.Expressions;
using System.Reflection;

using MimironSQL.Formats;
using MimironSQL.EntityFrameworkCore.Db2.Schema;
using MimironSQL.EntityFrameworkCore.Extensions;
using MimironSQL.EntityFrameworkCore.Db2.Model;

namespace MimironSQL.EntityFrameworkCore.Db2.Query;

internal static class Db2BatchedNavigationProjector
{
    public static IEnumerable<TResult> Project<TEntity, TResult, TRow>(
        IEnumerable<TEntity> source,
        Db2Model model,
        Func<string, (IDb2File<TRow> File, Db2TableSchema Schema)> tableResolver,
        Db2NavigationMemberAccessPlan[] accesses,
        Expression<Func<TEntity, TResult>> selector,
        int? take)
        where TRow : struct, IRowHandle
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

                var fieldSchema = target.ResolveFieldSchema(member, context: $"batched navigation projection on '{join.Navigation.SourceClrType.FullName}.{join.Navigation.NavigationMember.Name}'");
                accessorByMember[member] = new Db2FieldAccessor(fieldSchema);
            }

            var keys = keysByNavigation[navMember];
            Dictionary<int, TRow> rowsByKey = new(capacity: Math.Min(keys.Count, relatedFile.RecordsCount));

            if (keys is { Count: not 0 })
            {
                foreach (var row in relatedFile.EnumerateRows())
                {
                    var rowId = Db2RowHandleAccess.AsHandle(row).RowId;
                    if (!keys.Contains(rowId))
                        continue;

                    rowsByKey[rowId] = row;
                }
            }

            lookupByNavigation[navMember] = new NavigationLookup<TRow>(relatedFile, rowsByKey, accessorByMember);
        }

        var rewriter = new SelectorRewriter<TRow>(selector.Parameters[0], lookupByNavigation);
        var rewrittenBody = rewriter.Visit(selector.Body)!;

        // Wrap with navigation locals: each navigation gets keyVar, rowVar, foundVar initialized once
        var navLocals = rewriter.NavigationLocals;
        if (navLocals is { Count: not 0 })
        {
            var variables = new List<ParameterExpression>(navLocals.Count * 3);
            var assignments = new List<Expression>(navLocals.Count * 3);

            foreach (var (navMember, locals) in navLocals)
            {
                var plans = accessGroups[navMember];
                var join = plans[0].Join;
                var lookup = lookupByNavigation[navMember];

                variables.Add(locals.KeyVar);
                variables.Add(locals.RowVar);
                variables.Add(locals.FoundVar);

                // key = entity.FkMember (converted to int)
                var keyExpr = join.RootKeyMember.CreateInt32KeyExpression(selector.Parameters[0]);
                assignments.Add(Expression.Assign(locals.KeyVar, keyExpr));

                // found = lookup.TryGetRow(key, out row)
                var tryGetMethod = typeof(NavigationLookup<TRow>).GetMethod(nameof(NavigationLookup<>.TryGetRow))!;
                var lookupExpr = Expression.Constant(lookup);
                var tryGetCall = Expression.Call(lookupExpr, tryGetMethod, locals.KeyVar, locals.RowVar);
                assignments.Add(Expression.Assign(locals.FoundVar, tryGetCall));
            }

            assignments.Add(rewrittenBody);
            rewrittenBody = Expression.Block(variables, assignments);
        }

        var projection = Expression.Lambda<Func<TEntity, TResult>>(rewrittenBody, selector.Parameters[0]).Compile();

        foreach (var entity in buffered)
            yield return projection(entity);
    }

    private static Func<TEntity, int> CreateIntGetter<TEntity>(MemberInfo member)
    {
        if (member is not PropertyInfo { GetMethod.IsPublic: true } p)
            throw new NotSupportedException($"Key member '{member.Name}' must be a public property.");

        var entity = Expression.Parameter(typeof(TEntity), "entity");

        var access = Expression.Property(entity, p);

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

    private sealed class NavigationLookup<TRow>(IDb2File file, Dictionary<int, TRow> rowsByKey, Dictionary<MemberInfo, Db2FieldAccessor> accessorByMember)
        where TRow : struct, IRowHandle
    {
        public IDb2File File { get; } = file;

        public bool TryGetRow(int key, out TRow row)
        {
            return rowsByKey.TryGetValue(key, out row);
        }

        public Db2FieldAccessor GetAccessor(MemberInfo member)
            => accessorByMember[member];

        public T ReadField<T>(TRow row, int fieldIndex)
            => Db2RowHandleAccess.ReadField<TRow, T>(File, row, fieldIndex);
    }

    internal sealed record NavigationLocalVars(ParameterExpression KeyVar, ParameterExpression RowVar, ParameterExpression FoundVar);

    private sealed class SelectorRewriter<TRow>(
        ParameterExpression entityParam,
        Dictionary<MemberInfo, NavigationLookup<TRow>> lookupByNavigation) : ExpressionVisitor
        where TRow : struct, IRowHandle
    {
        public Dictionary<MemberInfo, NavigationLocalVars> NavigationLocals { get; } = [];

        protected override Expression VisitMember(MemberExpression node)
        {
            node = (MemberExpression)base.VisitMember(node);

            // x.Nav.Member
            switch (node.Expression)
            {
                case MemberExpression { Member: PropertyInfo or FieldInfo } nav when nav.Expression == entityParam:
                    {
                        if (!lookupByNavigation.TryGetValue(nav.Member, out var lookup))
                            return node;

                        // Get or create shared locals for this navigation
                        if (!NavigationLocals.TryGetValue(nav.Member, out var locals))
                        {
                            var navName = nav.Member.Name;
                            locals = new NavigationLocalVars(
                                Expression.Variable(typeof(int), $"{navName}_key"),
                                Expression.Variable(typeof(TRow), $"{navName}_row"),
                                Expression.Variable(typeof(bool), $"{navName}_found"));
                            NavigationLocals[nav.Member] = locals;
                        }

                        var accessor = lookup.GetAccessor(node.Member);
                        var lookupExpression = Expression.Constant(lookup);

                        // Just use the pre-initialized locals: if (found) read else default
                        var read = BuildReadExpression(lookupExpression, locals.RowVar, accessor, node.Type);
                        var defaultValue = Expression.Default(node.Type);

                        return Expression.Condition(locals.FoundVar, read, defaultValue);
                    }

                default:
                    return node;
            }
        }

        private static Expression BuildReadExpression(ConstantExpression lookupExpression, Expression rowExpression, Db2FieldAccessor accessor, Type targetType)
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
                var readArrayMethod = typeof(NavigationLookup<TRow>)
                    .GetMethod(nameof(NavigationLookup<>.ReadField), BindingFlags.Public | BindingFlags.Instance)!
                    .MakeGenericMethod(arrayType);

                var readArray = Expression.Call(lookupExpression, readArrayMethod, rowExpression, Expression.Constant(accessor.Field.ColumnStartIndex));
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
            var readMethod = typeof(NavigationLookup<TRow>)
                .GetMethod(nameof(NavigationLookup<>.ReadField), BindingFlags.Public | BindingFlags.Instance)!
                .MakeGenericMethod(readType);

            var read = Expression.Call(lookupExpression, readMethod, rowExpression, Expression.Constant(accessor.Field.ColumnStartIndex));

            return targetType.IsNullable() ? Expression.Convert(read, targetType) : read;
        }
    }
}
