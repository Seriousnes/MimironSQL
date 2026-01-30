using System.Linq.Expressions;
using System.Reflection;

using MimironSQL.Db2;
using MimironSQL.Db2.Model;
using MimironSQL.Db2.Schema;
using MimironSQL.Formats;
using MimironSQL.Extensions;

namespace MimironSQL.Db2.Query;

internal static class Db2NavigationRowProjector
{
    public static IEnumerable<TResult> ProjectFromRows<TResult, TRow>(
        IDb2File rootFile,
        IEnumerable<TRow> rows,
        Db2TableSchema rootSchema,
        Db2Model model,
        Func<string, (IDb2File<TRow> File, Db2TableSchema Schema)> tableResolver,
        Db2NavigationMemberAccessPlan[] accesses,
        LambdaExpression selector,
        int? take)
        where TRow : struct, IRowHandle
    {
        var accessGroups = accesses
            .GroupBy(a => a.Join.Navigation.NavigationMember)
            .ToDictionary(g => g.Key, g => g.ToArray());

        var buffered = new List<TRow>(take is { } t ? t : 16);
        var keysByNavigation = new Dictionary<MemberInfo, HashSet<int>>();
        var rootKeyFieldIndexByNavigation = new Dictionary<MemberInfo, int>();

        foreach (var (navMember, navAccesses) in accessGroups)
        {
            keysByNavigation[navMember] = [];
            var keyFieldSchema = navAccesses[0].Join.RootKeyFieldSchema;
            rootKeyFieldIndexByNavigation[navMember] = keyFieldSchema.ColumnStartIndex;
        }

        foreach (var row in rows)
        {
            buffered.Add(row);

            foreach (var (navMember, keys) in keysByNavigation)
            {
                var key = Db2RowHandleAccess.ReadField<TRow, int>(rootFile, row, rootKeyFieldIndexByNavigation[navMember]);
                if (key != 0)
                    keys.Add(key);
            }
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
                    var rowId = Db2RowHandleAccess.AsHandle(row).RowId;
                    if (!keys.Contains(rowId))
                        continue;

                    rowsByKey[rowId] = row;
                }
            }

            lookupByNavigation[navMember] = new NavigationLookup<TRow>(relatedFile, rowsByKey, accessorByMember);
        }

        var projectorCompiler = new RowProjectorCompiler<TRow>(rootFile, rootSchema, selector, lookupByNavigation, rootKeyFieldIndexByNavigation);
        var projector = projectorCompiler.Compile<TResult>();

        foreach (var row in buffered)
            yield return projector(row);
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

    private sealed class RowProjectorCompiler<TRow>(
        IDb2File rootFile,
        Db2TableSchema rootSchema,
        LambdaExpression selector,
        Dictionary<MemberInfo, NavigationLookup<TRow>> lookupByNavigation,
        Dictionary<MemberInfo, int> rootKeyFieldIndexByNavigation)
        where TRow : struct, IRowHandle
    {
        private readonly ParameterExpression _entityParam = selector.Parameters[0];
        private readonly ParameterExpression _rowParam = Expression.Parameter(typeof(TRow), "row");
        private readonly ConstantExpression _rootFileExpression = Expression.Constant(rootFile, typeof(IDb2File));

        public Func<TRow, TResult> Compile<TResult>()
        {
            var rewriter = new SelectorRewriter(
                _entityParam,
                _rowParam,
                _rootFileExpression,
                rootSchema,
                lookupByNavigation);

            var rewrittenBody = rewriter.Visit(selector.Body) ?? throw new NotSupportedException("Failed to rewrite selector for row-based projection.");

            // Wrap with navigation locals: each navigation gets keyVar, rowVar, foundVar initialized once
            var navLocals = rewriter.NavigationLocals;
            if (navLocals is { Count: not 0 })
            {
                var variables = new List<ParameterExpression>(navLocals.Count * 3);
                var assignments = new List<Expression>(navLocals.Count * 3);

                foreach (var (navMember, locals) in navLocals)
                {
                    var lookup = lookupByNavigation[navMember];
                    var keyFieldIndex = rootKeyFieldIndexByNavigation[navMember];

                    variables.Add(locals.KeyVar);
                    variables.Add(locals.RowVar);
                    variables.Add(locals.FoundVar);

                    // key = ReadField<int>(file, row, keyFieldIndex)
                    var readKeyMethod = typeof(Db2RowHandleAccess)
                        .GetMethod(nameof(Db2RowHandleAccess.ReadField), BindingFlags.Public | BindingFlags.Static)!
                        .MakeGenericMethod(typeof(TRow), typeof(int));
                    var readKey = Expression.Call(readKeyMethod, _rootFileExpression, _rowParam, Expression.Constant(keyFieldIndex));
                    assignments.Add(Expression.Assign(locals.KeyVar, readKey));

                    // found = lookup.TryGetRow(key, out row)
                    var tryGetMethod = typeof(NavigationLookup<TRow>).GetMethod(nameof(NavigationLookup<TRow>.TryGetRow))!;
                    var lookupExpr = Expression.Constant(lookup);
                    var tryGetCall = Expression.Call(lookupExpr, tryGetMethod, locals.KeyVar, locals.RowVar);
                    assignments.Add(Expression.Assign(locals.FoundVar, tryGetCall));
                }

                assignments.Add(rewrittenBody);
                rewrittenBody = Expression.Block(variables, assignments);
            }

            return Expression.Lambda<Func<TRow, TResult>>(rewrittenBody, _rowParam).Compile();
        }

        internal sealed record NavigationLocalVars(ParameterExpression KeyVar, ParameterExpression RowVar, ParameterExpression FoundVar);

        private sealed class SelectorRewriter(
            ParameterExpression entityParam,
            ParameterExpression rowParam,
            ConstantExpression rootFileExpression,
            Db2TableSchema rootSchema,
            Dictionary<MemberInfo, NavigationLookup<TRow>> lookupByNavigation) : ExpressionVisitor
        {
            public Dictionary<MemberInfo, NavigationLocalVars> NavigationLocals { get; } = [];

            protected override Expression VisitMember(MemberExpression node)
            {
                // x.Nav.Member - handle navigation member access BEFORE visiting base
                if (node.Expression is MemberExpression { Member: PropertyInfo or FieldInfo } nav && nav.Expression == entityParam && lookupByNavigation.TryGetValue(nav.Member, out var lookup))
                {
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
                    var read = BuildLookupReadExpression(lookupExpression, locals.RowVar, accessor, node.Type);
                    var defaultValue = Expression.Default(node.Type);

                    return Expression.Condition(locals.FoundVar, read, defaultValue);
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
                    return BuildReadExpression(rootFileExpression, rowParam, accessor, node.Type);
                }

                return node;
            }

            private static Expression BuildReadExpression(ConstantExpression rootFileExpression, Expression rowExpression, Db2FieldAccessor accessor, Type targetType)
            {
                var readType = targetType.UnwrapNullable();

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
                    var readArrayMethod = typeof(Db2RowHandleAccess)
                        .GetMethod(nameof(Db2RowHandleAccess.ReadField), BindingFlags.Public | BindingFlags.Static)!
                        .MakeGenericMethod(typeof(TRow), arrayType);

                    var readArray = Expression.Call(readArrayMethod, rootFileExpression, rowExpression, Expression.Constant(accessor.Field.ColumnStartIndex));
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

                var readMethod = typeof(Db2RowHandleAccess)
                    .GetMethod(nameof(Db2RowHandleAccess.ReadField), BindingFlags.Public | BindingFlags.Static)!
                    .MakeGenericMethod(typeof(TRow), readType);

                var read = Expression.Call(readMethod, rootFileExpression, rowExpression, Expression.Constant(accessor.Field.ColumnStartIndex));
                return targetType.IsNullable() ? Expression.Convert(read, targetType) : read;
            }

            private static Expression BuildLookupReadExpression(Expression lookupExpression, Expression rowExpression, Db2FieldAccessor accessor, Type targetType)
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
                        .GetMethod(nameof(NavigationLookup<TRow>.ReadField), BindingFlags.Public | BindingFlags.Instance)!
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
                    .GetMethod(nameof(NavigationLookup<TRow>.ReadField), BindingFlags.Public | BindingFlags.Instance)!
                    .MakeGenericMethod(readType);

                var read = Expression.Call(lookupExpression, readMethod, rowExpression, Expression.Constant(accessor.Field.ColumnStartIndex));
                return targetType.IsNullable() ? Expression.Convert(read, targetType) : read;
            }
        }
    }
}
