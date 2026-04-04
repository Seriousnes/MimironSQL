using System.Collections.Concurrent;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;

using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Storage;

using MimironSQL.Db2;
using MimironSQL.EntityFrameworkCore.Index;
using MimironSQL.EntityFrameworkCore.Infrastructure;
using MimironSQL.EntityFrameworkCore.Query.Internal.Expressions;
using MimironSQL.EntityFrameworkCore.Schema;
using MimironSQL.EntityFrameworkCore.Storage;
using MimironSQL.Formats;
using MimironSQL.Formats.Wdc5;
using MimironSQL.Formats.Wdc5.Db2;
using MimironSQL.Formats.Wdc5.Index;

namespace MimironSQL.EntityFrameworkCore.Query.Internal;

internal static class Db2TableEnumerator
{
    internal static IEnumerable<ValueBuffer> Table(QueryContext queryContext, Db2QueryExpression queryExpression)
    {
        var dbContext = queryContext.Context;

        var entityType = queryExpression.EntityType;
        var tableName = entityType.GetTableName() ?? entityType.ClrType.Name;

        var options = dbContext.GetService<IDbContextOptions>();
        var extension = options.FindExtension<MimironDb2OptionsExtension>();

        var wowVersion = extension?.WowVersion;
        if (string.IsNullOrWhiteSpace(wowVersion))
        {
            throw new InvalidOperationException("MimironDb2 WOW_VERSION is not configured. Call UseMimironDb2(o => o.WithWowVersion(...)).");
        }

        var store = dbContext.GetService<IMimironDb2Store>();
        // Prefer DI-provided format, but default to WDC5 for now.
        var format = dbContext.GetService<IDb2Format>() ?? new Wdc5Format();

        if (queryExpression.Joins.Count == 0)
        {
            var (typedFile, schema) = store.OpenTableWithSchema<RowHandle>(tableName);
            var layout = format.GetLayout(typedFile);

            if (Db2RowPredicateCompiler.TryGetVirtualIdPrimaryKeyEqualityLookup(entityType, tableName, schema, queryExpression.Predicates, out var getKeyValue, out var keyType))
            {
                var keyValue = getKeyValue(queryContext);
                if (keyValue is not null && TryGetRowById(typedFile, keyType, keyValue, out var handle))
                {
                    var fastPropertiesToRead = GetPropertiesToRead(entityType, additionalRequired: []);
                    var fastValueBufferLength = GetEntityValueBufferLength(entityType);
                    var fastReadPlan = ValueBufferReadPlanCache.GetOrCreate(entityType, tableName, wowVersion, layout.LayoutHash, schema, fastValueBufferLength, fastPropertiesToRead);

                    var values = fastValueBufferLength > 0 ? new object?[fastValueBufferLength] : Array.Empty<object?>();
                    foreach (var entry in fastReadPlan.Entries)
                    {
                        values[entry.PropertyIndex] = entry.Reader(typedFile, handle);
                    }

                    yield return new ValueBuffer(values);
                }

                yield break;
            }

            var rowPredicate = Db2RowPredicateCompiler.TryCompileRowHandlePredicate(entityType, tableName, schema, queryExpression.Predicates);
            var rowHandles = GetCandidateRowHandles(queryContext, queryExpression, tableName, extension, store, format, typedFile, schema, layout);

            var propertiesToRead = GetPropertiesToRead(entityType, additionalRequired: []);
            var valueBufferLength = GetEntityValueBufferLength(entityType);
            var readPlan = ValueBufferReadPlanCache.GetOrCreate(entityType, tableName, wowVersion, layout.LayoutHash, schema, valueBufferLength, propertiesToRead);

            foreach (var handle in rowHandles)
            {
                if (rowPredicate is not null && !rowPredicate(queryContext, typedFile, handle))
                {
                    continue;
                }

                var values = valueBufferLength > 0 ? new object?[valueBufferLength] : Array.Empty<object?>();

                foreach (var entry in readPlan.Entries)
                {
                    values[entry.PropertyIndex] = entry.Reader(typedFile, handle);
                }

                yield return new ValueBuffer(values);
            }

            yield break;
        }

        // Joins are represented as a left-deep tree on the query root.
        // Support Join/LeftJoin for Include/ThenInclude execution during bootstrap.
        var joins = queryExpression.Joins;

        var rootSchemaForJoin = store.GetSchema(tableName);

        var rootValueBufferLength = GetEntityValueBufferLength(entityType);
        var rootPropertiesToRead = GetPropertiesToRead(entityType, additionalRequired: []);

        var joinPlans = new List<JoinPlan>(joins.Count);

        // Slot 0 is the root entity, subsequent slots are join inners in order.
        var slotEntityTypes = new List<IEntityType>(capacity: joins.Count + 1) { entityType };
        var slotOffsets = new List<int>(capacity: joins.Count + 1) { 0 };
        var runningOffset = rootValueBufferLength;

        // Cache lookup materialization per (table,keyIndex) to avoid rebuilding for repeated includes.
        var lookupCache = new Dictionary<(string TableName, int KeyIndex), Dictionary<object, List<object?[]>>>(new LookupCacheKeyComparer());

        for (var joinIndex = 0; joinIndex < joins.Count; joinIndex++)
        {
            var (joinOperator, innerQuery, outerKeySelector, innerKeySelector) = joins[joinIndex];

            if (joinOperator is not (nameof(Queryable.Join) or nameof(Queryable.LeftJoin)))
            {
                throw new NotSupportedException($"MimironDb2 join execution currently only supports Queryable.Join and Queryable.LeftJoin. Saw '{joinOperator}'.");
            }

            if (innerQuery.Joins.Count != 0)
            {
                throw new NotSupportedException("MimironDb2 join execution does not currently support nested joins on the inner query.");
            }

            var innerEntityType = innerQuery.EntityType;
            var innerTableName = innerEntityType.GetTableName() ?? innerEntityType.ClrType.Name;

            var innerSchemaForJoin = store.GetSchema(innerTableName);
            var innerValueBufferLength = GetEntityValueBufferLength(innerEntityType);

            // Resolve key references.
            var outerKeyRef = ResolveJoinKeyReference(slotEntityTypes, outerKeySelector);
            var innerKeyProperty = GetJoinKeyProperty(innerEntityType, innerKeySelector);
            var innerKeyIndex = innerKeyProperty.GetIndex();

            var innerPropertiesToRead = GetPropertiesToRead(innerEntityType, additionalRequired: [innerKeyProperty]);

            // Build inner lookup.
            if (!lookupCache.TryGetValue((innerTableName, innerKeyIndex), out var innerLookup))
            {
                innerLookup = BuildLookup(
                    store,
                    format,
                    innerTableName,
                    innerSchemaForJoin,
                    wowVersion,
                    innerEntityType,
                    innerValueBufferLength,
                    innerPropertiesToRead,
                    innerKeyProperty);

                lookupCache.Add((innerTableName, innerKeyIndex), innerLookup);
            }

            joinPlans.Add(new JoinPlan(
                joinOperator,
                innerEntityType,
                innerTableName,
                innerValueBufferLength,
                innerPropertiesToRead,
                outerKeyRef,
                innerKeyIndex,
                innerLookup,
                runningOffset));

            slotEntityTypes.Add(innerEntityType);
            slotOffsets.Add(runningOffset);
            runningOffset += innerValueBufferLength;
        }

        var (rootFile, _) = store.OpenTableWithSchema<RowHandle>(tableName);
        var rootLayoutForJoin = format.GetLayout(rootFile);

        var rootRowPredicate = Db2RowPredicateCompiler.TryCompileRowHandlePredicate(entityType, tableName, rootSchemaForJoin, queryExpression.Predicates);

        var rootReadPlan = ValueBufferReadPlanCache.GetOrCreate(entityType, tableName, wowVersion, rootLayoutForJoin.LayoutHash, rootSchemaForJoin, rootValueBufferLength, rootPropertiesToRead);
        foreach (var rootHandle in rootFile.EnumerateRowHandles())
        {
            if (rootRowPredicate is not null && !rootRowPredicate(queryContext, rootFile, rootHandle))
            {
                continue;
            }

            var rootValues = rootValueBufferLength > 0 ? new object?[rootValueBufferLength] : Array.Empty<object?>();
            foreach (var entry in rootReadPlan.Entries)
            {
                rootValues[entry.PropertyIndex] = entry.Reader(rootFile, rootHandle);
            }

            // Apply joins in order, expanding rows for one-to-many.
            var currentRows = new List<object?[]>(capacity: 1) { rootValues };

            foreach (var plan in joinPlans)
            {
                var nextRows = new List<object?[]>();

                foreach (var row in currentRows)
                {
                    var outerKey = GetValue(row, plan.OuterKeyRef);

                    if (outerKey is null)
                    {
                        if (plan.JoinOperator == nameof(Queryable.LeftJoin))
                        {
                            nextRows.Add(Concat(row, plan.Offset, plan.InnerValueBufferLength, null));
                        }

                        continue;
                    }

                    if (!plan.InnerLookup.TryGetValue(outerKey, out var innerMatches) || innerMatches.Count == 0)
                    {
                        if (plan.JoinOperator == nameof(Queryable.LeftJoin))
                        {
                            nextRows.Add(Concat(row, plan.Offset, plan.InnerValueBufferLength, null));
                        }

                        continue;
                    }

                    foreach (var inner in innerMatches)
                    {
                        nextRows.Add(Concat(row, plan.Offset, plan.InnerValueBufferLength, inner));
                    }
                }

                currentRows = nextRows;
                if (currentRows.Count == 0)
                {
                    break;
                }
            }

            foreach (var combined in currentRows)
            {
                var final = combined;
                if (combined.Length != runningOffset)
                {
                    // Ensure the ValueBuffer matches the joined layout even when there are no joins.
                    final = combined;
                }

                yield return new ValueBuffer(final);
            }
        }
    }

    private static IEnumerable<RowHandle> GetCandidateRowHandles(
        QueryContext queryContext,
        Db2QueryExpression queryExpression,
        string tableName,
        MimironDb2OptionsExtension? extension,
        IMimironDb2Store store,
        IDb2Format format,
        IDb2File<RowHandle> typedFile,
        Db2TableSchema schema,
        Db2FileLayout layout)
    {
        if (extension is not { EnableCustomIndexes: true } || typedFile is not Wdc5File wdc5File)
        {
            return typedFile.EnumerateRowHandles();
        }

        if (!Db2RowPredicateCompiler.TryGetSingleScalarFieldIndexHint(
                queryExpression.EntityType,
                tableName,
                schema,
                queryExpression.Predicates,
                out var fieldSchema,
            out var getValue))
        {
            return typedFile.EnumerateRowHandles();
        }

        try
        {
            var dbContext = queryContext.Context;
            dbContext.GetService<Db2IndexBuilder>().EnsureBuilt(dbContext.Model, store, format);

            var value = getValue(queryContext);
            if (value is null)
            {
                return typedFile.EnumerateRowHandles();
            }

            var encodedValue = Db2IndexValueEncoder.EncodeObject(
                value,
                fieldSchema.ValueType,
                Db2IndexBuilder.GetValueByteWidth(typedFile, fieldSchema.ColumnStartIndex));

            var indexedHandles = dbContext.GetService<Db2IndexLookup>().TryFindEquals(tableName, fieldSchema, layout.LayoutHash, encodedValue);
            if (indexedHandles is null)
            {
                return typedFile.EnumerateRowHandles();
            }

            return indexedHandles.Select(handle => wdc5File.GetRowHandle(handle.SectionIndex, handle.RowIndexInSection));
        }
        catch
        {
            return typedFile.EnumerateRowHandles();
        }
    }

    private static bool TryGetRowById(IDb2File<RowHandle> file, Type idType, object idValue, out RowHandle handle)
    {
        ArgumentNullException.ThrowIfNull(file);
        ArgumentNullException.ThrowIfNull(idType);

        if (idValue is null)
        {
            handle = default;
            return false;
        }

        var method = typeof(IDb2File<RowHandle>)
            .GetMethods(BindingFlags.Public | BindingFlags.Instance)
            .Single(m => m.Name == nameof(IDb2File<RowHandle>.TryGetRowById) && m.IsGenericMethodDefinition);

        var generic = method.MakeGenericMethod(idType);
        var args = new object?[] { idValue, null };
        var found = (bool)generic.Invoke(file, args)!;
        handle = found ? (RowHandle)args[1]! : default;
        return found;
    }

    private readonly record struct JoinKeyReference(int SlotIndex, int PropertyIndex, int SlotOffset)
    {
        public int AbsoluteIndex => SlotOffset + PropertyIndex;
    }

    private sealed record JoinPlan(
        string JoinOperator,
        IEntityType InnerEntityType,
        string InnerTableName,
        int InnerValueBufferLength,
        IProperty[] InnerPropertiesToRead,
        JoinKeyReference OuterKeyRef,
        int InnerKeyIndex,
        Dictionary<object, List<object?[]>> InnerLookup,
        int Offset);

    private static int GetEntityValueBufferLength(IEntityType entityType)
        => !entityType.GetProperties().Any() ? 0 : entityType.GetProperties().Max(static p => p.GetIndex()) + 1;

    private static IProperty[] GetPropertiesToRead(IEntityType entityType, IReadOnlyCollection<IProperty> additionalRequired)
    {
        var set = new HashSet<IProperty>();

        foreach (var p in entityType.GetProperties())
        {
            if (p.IsShadowProperty())
            {
                continue;
            }

            if (p.PropertyInfo is null)
            {
                continue;
            }

            set.Add(p);
        }

        foreach (var p in additionalRequired)
        {
            set.Add(p);
        }

        return set.OrderBy(static p => p.GetIndex()).ToArray();
    }

    private static Dictionary<object, List<object?[]>> BuildLookup(
        IMimironDb2Store store,
        IDb2Format format,
        string tableName,
        Db2TableSchema schema,
        string wowVersion,
        IEntityType entityType,
        int valueBufferLength,
        IProperty[] propertiesToRead,
        IProperty keyProperty)
    {
        var keyIndex = keyProperty.GetIndex();
        var lookup = new Dictionary<object, List<object?[]>>();

        var (file, _) = store.OpenTableWithSchema<RowHandle>(tableName);
        var layout = format.GetLayout(file);

        var readPlan = ValueBufferReadPlanCache.GetOrCreate(entityType, tableName, wowVersion, layout.LayoutHash, schema, valueBufferLength, propertiesToRead);
        foreach (var handle in file.EnumerateRowHandles())
        {
            var values = valueBufferLength > 0 ? new object?[valueBufferLength] : Array.Empty<object?>();

            foreach (var entry in readPlan.Entries)
            {
                values[entry.PropertyIndex] = entry.Reader(file, handle);
            }

            var key = values.Length > keyIndex ? values[keyIndex] : null;
            if (key is null)
            {
                continue;
            }

            if (!lookup.TryGetValue(key, out var list))
            {
                list = [];
                lookup.Add(key, list);
            }

            list.Add(values);
        }

        return lookup;
    }

    private static JoinKeyReference ResolveJoinKeyReference(IReadOnlyList<IEntityType> slotEntityTypes, LambdaExpression outerKeySelector)
    {
        ArgumentNullException.ThrowIfNull(slotEntityTypes);
        ArgumentNullException.ThrowIfNull(outerKeySelector);

        if (outerKeySelector.Parameters.Count != 1)
        {
            throw new NotSupportedException("MimironDb2 join key selectors must have exactly one parameter.");
        }

        var parameter = outerKeySelector.Parameters[0];
        var body = outerKeySelector.Body;
        while (body is UnaryExpression { NodeType: ExpressionType.Convert or ExpressionType.ConvertChecked } u)
        {
            body = u.Operand;
        }

        string propertyName;
        Expression instance;

        if (body is MethodCallExpression
            {
                Method.Name: nameof(EF.Property),
                Arguments: [var inst, ConstantExpression { Value: string s }]
            })
        {
            instance = inst;
            propertyName = s;
        }
        else if (body is MemberExpression { Member: PropertyInfo p, Expression: { } instExpr })
        {
            instance = instExpr;
            propertyName = p.Name;
        }
        else
        {
            throw new NotSupportedException($"MimironDb2 join execution only supports simple property key selectors. Saw '{outerKeySelector.Body}'.");
        }

        var slotIndex = ResolveSlotIndexFromInstance(parameter, instance);
        if ((uint)slotIndex >= (uint)slotEntityTypes.Count)
        {
            throw new NotSupportedException($"MimironDb2 could not resolve join key slot index '{slotIndex}' (slots={slotEntityTypes.Count}).");
        }

        var entityType = slotEntityTypes[slotIndex];
        var property = entityType.FindProperty(propertyName)
            ?? throw new NotSupportedException($"MimironDb2 could not resolve join key property '{propertyName}' on '{entityType.DisplayName()}'.");

        // Slot offsets are a fixed span layout: sum of entity value buffer lengths for prior slots.
        var slotOffset = 0;
        for (var i = 0; i < slotIndex; i++)
        {
            slotOffset += GetEntityValueBufferLength(slotEntityTypes[i]);
        }

        return new JoinKeyReference(slotIndex, property.GetIndex(), slotOffset);
    }

    private static int ResolveSlotIndexFromInstance(ParameterExpression parameter, Expression instance)
    {
        // instance is expected to be either the parameter itself (root entity) or a chain of .Outer/.Inner
        // member accesses over EF Core's nested TransparentIdentifier type.
        if (instance == parameter)
        {
            if (Db2RowPredicateCompiler.TryGetTransparentIdentifierTypes(parameter.Type, out _, out _))
            {
                throw new NotSupportedException("MimironDb2 cannot use the entire TransparentIdentifier as a join key instance.");
            }

            return 0;
        }

        var steps = new List<string>();
        var current = instance;
        while (current != parameter)
        {
            if (current is not MemberExpression { Expression: { } parent, Member.Name: var name }
                || (name != "Outer" && name != "Inner"))
            {
                throw new NotSupportedException($"MimironDb2 cannot resolve join key instance '{instance}'.");
            }

            steps.Add(name);
            current = parent;
        }

        steps.Reverse();

        var type = parameter.Type;
        if (!Db2RowPredicateCompiler.TryGetTransparentIdentifierTypes(type, out _, out _))
        {
            throw new NotSupportedException("MimironDb2 expected a TransparentIdentifier parameter for this join key selector.");
        }

        var baseIndex = 0;
        foreach (var step in steps)
        {
            if (!Db2RowPredicateCompiler.TryGetTransparentIdentifierTypes(type, out var outerType, out var innerType))
            {
                throw new NotSupportedException($"MimironDb2 cannot traverse '{step}' on non-TransparentIdentifier type '{type.FullName}'.");
            }

            if (step == "Outer")
            {
                type = outerType;
                continue;
            }

            // Inner: select the leaf index of the current inner.
            var outerLeaves = CountTransparentIdentifierLeaves(outerType);
            return baseIndex + outerLeaves;
        }

        // Ending on the outer-most leaf.
        return baseIndex;
    }

    private static int CountTransparentIdentifierLeaves(Type type)
        => Db2RowPredicateCompiler.TryGetTransparentIdentifierTypes(type, out var outer, out _) ? CountTransparentIdentifierLeaves(outer) + 1 : 1;

    private static object? GetValue(object?[] row, JoinKeyReference keyRef)
    {
        var index = keyRef.AbsoluteIndex;
        return (uint)index < (uint)row.Length ? row[index] : null;
    }

    private static object?[] Concat(object?[] outer, int offset, int innerLength, object?[]? inner)
    {
        var total = offset + innerLength;
        if (outer.Length != offset)
        {
            // Outer may already include previous joins; offset is the current outer span length.
            total = outer.Length + innerLength;
            offset = outer.Length;
        }

        var combined = total > 0 ? new object?[total] : Array.Empty<object?>();
        if (outer.Length > 0)
        {
            Array.Copy(outer, 0, combined, 0, outer.Length);
        }

        if (inner is not null && innerLength > 0)
        {
            Array.Copy(inner, 0, combined, offset, Math.Min(innerLength, inner.Length));
        }

        // If inner is null (LeftJoin no match), keep the inner segment as all-null.
        return combined;
    }

    private sealed class LookupCacheKeyComparer : IEqualityComparer<(string TableName, int KeyIndex)>
    {
        public bool Equals((string TableName, int KeyIndex) x, (string TableName, int KeyIndex) y)
            => StringComparer.OrdinalIgnoreCase.Equals(x.TableName, y.TableName) && x.KeyIndex == y.KeyIndex;

        public int GetHashCode((string TableName, int KeyIndex) obj)
            => HashCode.Combine(StringComparer.OrdinalIgnoreCase.GetHashCode(obj.TableName), obj.KeyIndex);
    }

    private static IProperty GetJoinKeyProperty(IEntityType entityType, LambdaExpression keySelector)
    {
        ArgumentNullException.ThrowIfNull(entityType);
        ArgumentNullException.ThrowIfNull(keySelector);

        var body = keySelector.Body;
        while (body is UnaryExpression { NodeType: ExpressionType.Convert or ExpressionType.ConvertChecked } u)
        {
            body = u.Operand;
        }

        if (body is MethodCallExpression
            {
                Method.Name: nameof(EF.Property),
                Arguments: [_, ConstantExpression { Value: string propertyName }]
            })
        {
            return entityType.FindProperty(propertyName)
                ?? throw new NotSupportedException($"MimironDb2 could not resolve join key property '{propertyName}' on '{entityType.DisplayName()}'.");
        }

        if (body is not MemberExpression { Member: PropertyInfo prop })
        {
            throw new NotSupportedException($"MimironDb2 join execution only supports simple property key selectors. Saw '{keySelector.Body}'.");
        }

        return entityType.FindProperty(prop.Name)
            ?? throw new NotSupportedException($"MimironDb2 could not resolve join key property '{prop.Name}' on '{entityType.DisplayName()}'.");
    }

    private readonly record struct ValueBufferReadPlanEntry(int PropertyIndex, Func<IDb2File, RowHandle, object?> Reader);

    private sealed class ValueBufferReadPlan
    {
        public required int ValueBufferLength { get; init; }
        public required ValueBufferReadPlanEntry[] Entries { get; init; }
    }

    private sealed class ValueBufferReadPlanCache
    {
        private readonly record struct CacheKey(
            IEntityType EntityType,
            string TableName,
            string WowVersion,
            uint LayoutHash,
            string PropertySetKey);

        private static readonly ConcurrentDictionary<CacheKey, ValueBufferReadPlan> Cache = new();

        private readonly record struct ReaderCacheKey(Type ReadType, Type ResultType, int FieldIndex);

        private static readonly ConcurrentDictionary<ReaderCacheKey, Func<IDb2File, RowHandle, object?>> ReaderCache = new();

        public static ValueBufferReadPlan GetOrCreate(
            IEntityType entityType,
            string tableName,
            string wowVersion,
            uint layoutHash,
            Db2TableSchema schema,
            int valueBufferLength,
            IReadOnlyList<IProperty> propertiesToRead)
        {
            ArgumentNullException.ThrowIfNull(entityType);
            ArgumentNullException.ThrowIfNull(tableName);
            ArgumentNullException.ThrowIfNull(wowVersion);
            ArgumentNullException.ThrowIfNull(schema);
            ArgumentNullException.ThrowIfNull(propertiesToRead);

            var ordered = propertiesToRead.Count == 0
                ? Array.Empty<IProperty>()
                : propertiesToRead.OrderBy(static p => p.GetIndex()).ToArray();

            var propertySetKey = CreatePropertySetKey(ordered);
            var key = new CacheKey(entityType, tableName, wowVersion, layoutHash, propertySetKey);

            return Cache.GetOrAdd(key, _ => Build(entityType, tableName, schema, valueBufferLength, ordered));
        }

        private static ValueBufferReadPlan Build(
            IEntityType entityType,
            string tableName,
            Db2TableSchema schema,
            int valueBufferLength,
            IReadOnlyList<IProperty> orderedPropertiesToRead)
        {
            var storeObject = StoreObjectIdentifier.Table(tableName, schema: null);
            var entries = new List<ValueBufferReadPlanEntry>(capacity: orderedPropertiesToRead.Count);

            foreach (var property in orderedPropertiesToRead)
            {
                var columnName = property.GetColumnName(storeObject) ?? property.GetColumnName() ?? property.Name;
                if (!schema.TryGetFieldCaseInsensitive(columnName, out var fieldSchema))
                {
                    continue;
                }

                var resultType = property.ClrType;
                var readType = GetReadTypeForReadField(resultType);
                var reader = ReaderCache.GetOrAdd(new ReaderCacheKey(readType, resultType, fieldSchema.ColumnStartIndex), static key => CompileReader(key.ReadType, key.ResultType, key.FieldIndex));
                entries.Add(new ValueBufferReadPlanEntry(property.GetIndex(), reader));
            }

            return new ValueBufferReadPlan
            {
                ValueBufferLength = valueBufferLength,
                Entries = entries.ToArray(),
            };
        }

        private static Type GetReadTypeForReadField(Type resultType)
        {
            ArgumentNullException.ThrowIfNull(resultType);

            if (resultType.IsArray)
            {
                return resultType;
            }

            // For nullable properties, ReadField<T> expects the underlying non-nullable type.
            var unwrapped = Nullable.GetUnderlyingType(resultType);
            return unwrapped ?? resultType;
        }

        private static Func<IDb2File, RowHandle, object?> CompileReader(Type readType, Type resultType, int fieldIndex)
        {
            var file = Expression.Parameter(typeof(IDb2File), "file");
            var handle = Expression.Parameter(typeof(RowHandle), "handle");

            var readMethod = typeof(IDb2File)
                .GetMethod(nameof(IDb2File.ReadField), BindingFlags.Instance | BindingFlags.Public)!
                .MakeGenericMethod(readType);

            Expression readCall = Expression.Call(file, readMethod, handle, Expression.Constant(fieldIndex));
            if (readType != resultType)
            {
                readCall = Expression.Convert(readCall, resultType);
            }

            var boxed = Expression.Convert(readCall, typeof(object));
            return Expression.Lambda<Func<IDb2File, RowHandle, object?>>(boxed, file, handle).Compile();
        }

        private static string CreatePropertySetKey(IReadOnlyList<IProperty> orderedProperties)
        {
            if (orderedProperties.Count == 0)
            {
                return string.Empty;
            }

            var sb = new StringBuilder(capacity: orderedProperties.Count * 4);
            for (var i = 0; i < orderedProperties.Count; i++)
            {
                if (i != 0)
                {
                    sb.Append(',');
                }

                sb.Append(orderedProperties[i].GetIndex());
            }

            return sb.ToString();
        }
    }
}
