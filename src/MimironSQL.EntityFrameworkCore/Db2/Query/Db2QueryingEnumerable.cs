using System.Collections;
using System.Linq.Expressions;

using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Storage;

using MimironSQL.EntityFrameworkCore.Db2.Query.Expressions;
using MimironSQL.EntityFrameworkCore.Db2.Schema;
using MimironSQL.EntityFrameworkCore.Storage;
using MimironSQL.Formats;

namespace MimironSQL.EntityFrameworkCore.Db2.Query;

/// <summary>
/// Runtime enumerable that executes DB2 reads based on a <see cref="Db2QueryExecutionPlan"/>.
/// Analogous to InMemory's <c>QueryingEnumerable&lt;T&gt;</c>.
/// </summary>
internal sealed class Db2QueryingEnumerable<T>(
    QueryContext queryContext,
    Db2QueryExecutionPlan plan,
    Func<QueryContext, IDb2File, RowHandle, T> shaper) : IEnumerable<T>, IAsyncEnumerable<T>
{
    public IEnumerator<T> GetEnumerator()
        => new Enumerator(queryContext, plan, shaper);

    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

    public IAsyncEnumerator<T> GetAsyncEnumerator(CancellationToken cancellationToken = default)
        => new AsyncEnumerator(GetEnumerator(), cancellationToken);

    private sealed class Enumerator(
        QueryContext queryContext,
        Db2QueryExecutionPlan plan,
        Func<QueryContext, IDb2File, RowHandle, T> shaper) : IEnumerator<T>
    {
        private IEnumerator<RowHandle>? _inner;
        private IDb2File? _file;
        private bool _initialized;

        public T Current { get; private set; } = default!;
        object? IEnumerator.Current => Current;

        public bool MoveNext()
        {
            if (!_initialized)
            {
                Initialize();
                _initialized = true;
            }

            if (_inner is null)
                return false;

            if (!_inner.MoveNext())
            {
                Current = default!;
                return false;
            }

            Current = shaper(queryContext, _file!, _inner.Current);
            return true;
        }

        private void Initialize()
        {
            var db2Context = (Db2QueryContext)queryContext;
            var (file, schema) = db2Context.Store.OpenTableWithSchema(plan.TableName);
            _file = file;

            IEnumerable<RowHandle> handles = plan.Strategy switch
            {
                Db2ExecutionStrategy.PrimaryKeyLookup => EnumeratePkLookup(file, plan.PrimaryKeyIds!),
                Db2ExecutionStrategy.PrimaryKeyMultiLookup => EnumeratePkLookup(file, plan.PrimaryKeyIds!),
                Db2ExecutionStrategy.RuntimePrimaryKeyLookup => EnumerateRuntimePkLookup(file, queryContext, plan.PrimaryKeyParameterName!),
                Db2ExecutionStrategy.RuntimePrimaryKeyMultiLookup => EnumerateRuntimePkMultiLookup(file, queryContext, plan.PrimaryKeyParameterName!),
                Db2ExecutionStrategy.FullScan => EnumerateFullScan(file, schema, plan, queryContext),
                _ => throw new NotSupportedException($"Unsupported execution strategy: {plan.Strategy}"),
            };

            // Apply ordering
            if (plan.Orderings is { Count: > 0 })
                handles = ApplyOrdering(handles, file, schema, plan.Orderings);

            // Resolve offset (constant or from parameter)
            var offset = plan.Offset
                ?? (plan.OffsetParameterName is not null
                    ? Convert.ToInt32(queryContext.Parameters[plan.OffsetParameterName])
                    : (int?)null);
            if (offset.HasValue)
                handles = handles.Skip(offset.Value);

            // Resolve limit (constant or from parameter)
            var limit = plan.Limit
                ?? (plan.LimitParameterName is not null
                    ? Convert.ToInt32(queryContext.Parameters[plan.LimitParameterName])
                    : (int?)null);
            if (limit.HasValue)
                handles = handles.Take(limit.Value);

            _inner = handles.GetEnumerator();
        }

        public void Reset() => throw new NotSupportedException();

        public void Dispose()
        {
            _inner?.Dispose();
        }
    }

    private sealed class AsyncEnumerator(
        IEnumerator<T> inner,
        CancellationToken cancellationToken) : IAsyncEnumerator<T>
    {
        public T Current => inner.Current;

        public ValueTask<bool> MoveNextAsync()
        {
            cancellationToken.ThrowIfCancellationRequested();
            return ValueTask.FromResult(inner.MoveNext());
        }

        public ValueTask DisposeAsync()
        {
            inner.Dispose();
            return ValueTask.CompletedTask;
        }
    }

    // ── Enumeration strategies ──

    private static IEnumerable<RowHandle> EnumeratePkLookup(IDb2File file, IReadOnlyList<int> ids)
    {
        for (var i = 0; i < ids.Count; i++)
        {
            if (file.TryGetRowHandle(ids[i], out var handle))
                yield return handle;
        }
    }

    private static IEnumerable<RowHandle> EnumerateRuntimePkLookup(IDb2File file, QueryContext queryContext, string parameterName)
    {
        var paramValue = queryContext.Parameters[parameterName];
        var id = Convert.ToInt32(paramValue);
        if (file.TryGetRowHandle(id, out var handle))
            yield return handle;
    }

    private static IEnumerable<RowHandle> EnumerateRuntimePkMultiLookup(IDb2File file, QueryContext queryContext, string parameterName)
    {
        var paramValue = queryContext.Parameters[parameterName];
        if (paramValue is IEnumerable enumerable)
        {
            foreach (var item in enumerable)
            {
                var id = Convert.ToInt32(item);
                if (file.TryGetRowHandle(id, out var handle))
                    yield return handle;
            }
        }
    }

    private static IEnumerable<RowHandle> EnumerateFullScan(IDb2File file, Db2TableSchema schema, Db2QueryExecutionPlan plan, QueryContext queryContext)
    {
        var db2Context = (Db2QueryContext)queryContext;
        var filter = plan.Filter is not null
            ? CompileFilter(plan.Filter, schema, queryContext, db2Context.Store)
            : null;

        foreach (var handle in file.EnumerateRowHandles())
        {
            if (filter is null || filter(file, handle))
                yield return handle;;
        }
    }

    /// <summary>
    /// Compiles a <see cref="Db2FilterExpression"/> tree into a runtime predicate.
    /// Runtime parameters are resolved from <paramref name="queryContext"/>.
    /// </summary>
    private static Func<IDb2File, RowHandle, bool> CompileFilter(
        Db2FilterExpression filter,
        Db2TableSchema schema,
        QueryContext queryContext,
        IMimironDb2Store store)
    {
        return filter switch
        {
            Db2ComparisonFilterExpression comparison => CompileComparison(comparison, schema, queryContext),
            Db2ContainsFilterExpression contains => CompileContains(contains, schema, queryContext),
            Db2AndFilterExpression and => CompileAnd(and, schema, queryContext, store),
            Db2OrFilterExpression or => CompileOr(or, schema, queryContext, store),
            Db2NotFilterExpression not => CompileNot(not, schema, queryContext, store),
            Db2NullCheckFilterExpression nullCheck => CompileNullCheck(nullCheck, schema),
            Db2StringMatchFilterExpression stringMatch => CompileStringMatch(stringMatch, schema, queryContext),
            Db2StringLengthFilterExpression stringLength => CompileStringLength(stringLength, schema),
            Db2ExistsSubqueryFilterExpression exists => CompileExistsSubquery(exists, schema, queryContext, store),
            _ => throw new NotSupportedException($"Unsupported filter expression type: {filter.GetType().Name}"),
        };
    }

    private static Func<IDb2File, RowHandle, bool> CompileComparison(Db2ComparisonFilterExpression comparison, Db2TableSchema schema, QueryContext queryContext)
    {
        var fieldIndex = ResolveFieldIndex(comparison.Field, schema);
        var fieldClrType = comparison.Field.Type;

        // Resolve runtime parameters from QueryContext
        var value = comparison.Value is Db2RuntimeParameter param
            ? queryContext.Parameters[param.Name]
            : comparison.Value;

        return comparison.ComparisonKind switch
        {
            ExpressionType.Equal => (file, handle) => Equals(ReadFieldBoxed(file, handle, fieldIndex, fieldClrType), value),
            ExpressionType.NotEqual => (file, handle) => !Equals(ReadFieldBoxed(file, handle, fieldIndex, fieldClrType), value),
            ExpressionType.GreaterThan => (file, handle) => CompareValues(ReadFieldBoxed(file, handle, fieldIndex, fieldClrType), value) > 0,
            ExpressionType.GreaterThanOrEqual => (file, handle) => CompareValues(ReadFieldBoxed(file, handle, fieldIndex, fieldClrType), value) >= 0,
            ExpressionType.LessThan => (file, handle) => CompareValues(ReadFieldBoxed(file, handle, fieldIndex, fieldClrType), value) < 0,
            ExpressionType.LessThanOrEqual => (file, handle) => CompareValues(ReadFieldBoxed(file, handle, fieldIndex, fieldClrType), value) <= 0,
            _ => throw new NotSupportedException($"Unsupported comparison kind: {comparison.ComparisonKind}"),
        };
    }

    private static Func<IDb2File, RowHandle, bool> CompileContains(Db2ContainsFilterExpression contains, Db2TableSchema schema, QueryContext queryContext)
    {
        var fieldIndex = ResolveFieldIndex(contains.Field, schema);
        var fieldClrType = contains.Field.Type;

        // Resolve values: either concrete or from a runtime parameter
        HashSet<object> values;
        if (contains.ValuesParameterName is not null)
        {
            var paramValue = queryContext.Parameters[contains.ValuesParameterName];
            values = [];
            if (paramValue is IEnumerable enumerable)
            {
                foreach (var item in enumerable)
                    values.Add(item);
            }
        }
        else
        {
            values = new(contains.Values!);
        }

        return (file, handle) =>
        {
            var fieldValue = ReadFieldBoxed(file, handle, fieldIndex, fieldClrType);
            return fieldValue is not null && values.Contains(fieldValue);
        };
    }

    private static Func<IDb2File, RowHandle, bool> CompileAnd(
        Db2AndFilterExpression and,
        Db2TableSchema schema,
        QueryContext queryContext,
        IMimironDb2Store store)
    {
        var left = CompileFilter(and.Left, schema, queryContext, store);
        var right = CompileFilter(and.Right, schema, queryContext, store);
        return (file, handle) => left(file, handle) && right(file, handle);
    }

    private static Func<IDb2File, RowHandle, bool> CompileOr(
        Db2OrFilterExpression or,
        Db2TableSchema schema,
        QueryContext queryContext,
        IMimironDb2Store store)
    {
        var left = CompileFilter(or.Left, schema, queryContext, store);
        var right = CompileFilter(or.Right, schema, queryContext, store);
        return (file, handle) => left(file, handle) || right(file, handle);
    }

    private static Func<IDb2File, RowHandle, bool> CompileNot(
        Db2NotFilterExpression not,
        Db2TableSchema schema,
        QueryContext queryContext,
        IMimironDb2Store store)
    {
        var inner = CompileFilter(not.Inner, schema, queryContext, store);
        return (file, handle) => !inner(file, handle);
    }

    private static Func<IDb2File, RowHandle, bool> CompileNullCheck(Db2NullCheckFilterExpression nullCheck, Db2TableSchema schema)
    {
        var fieldIndex = ResolveFieldIndex(nullCheck.Field, schema);
        var fieldClrType = nullCheck.Field.Type;

        if (nullCheck.IsNotNull)
            return (file, handle) => ReadFieldBoxed(file, handle, fieldIndex, fieldClrType) is not null;
        else
            return (file, handle) => ReadFieldBoxed(file, handle, fieldIndex, fieldClrType) is null;
    }

    private static Func<IDb2File, RowHandle, bool> CompileStringMatch(
        Db2StringMatchFilterExpression stringMatch,
        Db2TableSchema schema,
        QueryContext queryContext)
    {
        var fieldIndex = ResolveFieldIndex(stringMatch.Field, schema);
        var constantPattern = stringMatch.Pattern;
        var patternParameterName = stringMatch.PatternParameterName;

        string? ResolvePattern()
        {
            if (patternParameterName is null)
                return constantPattern;

            if (!queryContext.Parameters.TryGetValue(patternParameterName, out var raw) || raw is null)
                return null;

            return raw switch
            {
                string s => s,
                char c => c.ToString(),
                _ => raw.ToString(),
            };
        }

        return stringMatch.MatchKind switch
        {
            Db2StringMatchKind.Contains => (file, handle) =>
            {
                var val = file.ReadField<string>(handle, fieldIndex);
                var pattern = ResolvePattern();
                return val is not null && pattern is not null && val.Contains(pattern, StringComparison.OrdinalIgnoreCase);
            },
            Db2StringMatchKind.StartsWith => (file, handle) =>
            {
                var val = file.ReadField<string>(handle, fieldIndex);
                var pattern = ResolvePattern();
                return val is not null && pattern is not null && val.StartsWith(pattern, StringComparison.OrdinalIgnoreCase);
            },
            Db2StringMatchKind.EndsWith => (file, handle) =>
            {
                var val = file.ReadField<string>(handle, fieldIndex);
                var pattern = ResolvePattern();
                return val is not null && pattern is not null && val.EndsWith(pattern, StringComparison.OrdinalIgnoreCase);
            },
            _ => throw new NotSupportedException($"Unsupported string match kind: {stringMatch.MatchKind}"),
        };
    }

    private static Func<IDb2File, RowHandle, bool> CompileStringLength(Db2StringLengthFilterExpression stringLength, Db2TableSchema schema)
    {
        var fieldIndex = ResolveFieldIndex(stringLength.Field, schema);
        var value = stringLength.Value;

        return stringLength.ComparisonKind switch
        {
            ExpressionType.Equal => (file, handle) =>
            {
                var str = file.ReadField<string>(handle, fieldIndex);
                return (str?.Length ?? 0) == value;
            },
            ExpressionType.NotEqual => (file, handle) =>
            {
                var str = file.ReadField<string>(handle, fieldIndex);
                return (str?.Length ?? 0) != value;
            },
            ExpressionType.GreaterThan => (file, handle) =>
            {
                var str = file.ReadField<string>(handle, fieldIndex);
                return (str?.Length ?? 0) > value;
            },
            ExpressionType.GreaterThanOrEqual => (file, handle) =>
            {
                var str = file.ReadField<string>(handle, fieldIndex);
                return (str?.Length ?? 0) >= value;
            },
            ExpressionType.LessThan => (file, handle) =>
            {
                var str = file.ReadField<string>(handle, fieldIndex);
                return (str?.Length ?? 0) < value;
            },
            ExpressionType.LessThanOrEqual => (file, handle) =>
            {
                var str = file.ReadField<string>(handle, fieldIndex);
                return (str?.Length ?? 0) <= value;
            },
            _ => throw new NotSupportedException($"Unsupported comparison kind: {stringLength.ComparisonKind}"),
        };
    }

    private static Func<IDb2File, RowHandle, bool> CompileExistsSubquery(
        Db2ExistsSubqueryFilterExpression exists,
        Db2TableSchema principalSchema,
        QueryContext queryContext,
        IMimironDb2Store store)
    {
        // Get the principal key field index
        var principalKeyIndex = ResolveFieldIndex(exists.PrincipalKeyField, principalSchema);
        var principalKeyType = exists.PrincipalKeyField.Type;

        // Open the related table
        var (relatedFile, relatedSchema) = store.OpenTableWithSchema(exists.RelatedTableName);

        // Get the FK field index in the related table
        if (!relatedSchema.TryGetFieldCaseInsensitive(exists.ForeignKeyColumnName, out var fkField))
        {
            throw new InvalidOperationException(
                $"Foreign key column '{exists.ForeignKeyColumnName}' not found in related table '{exists.RelatedTableName}'.");
        }

        var fkFieldIndex = fkField.ColumnStartIndex;

        // Compile inner predicate if present
        Func<IDb2File, RowHandle, bool>? innerPredicate = null;
        if (exists.InnerPredicate is not null)
        {
            innerPredicate = CompileFilter(exists.InnerPredicate, relatedSchema, queryContext, store);
        }

        // Build a lookup of FK values to matching row existence
        // This is optimized for the case where we check multiple principal rows
        HashSet<int>? matchingFkValuesCache = null;

        return (principalFile, principalHandle) =>
        {
            // Get the principal key value
            var principalKeyValue = ReadFieldBoxed(principalFile, principalHandle, principalKeyIndex, principalKeyType);
            if (principalKeyValue is null)
                return false;

            var principalKeyInt = Convert.ToInt32(principalKeyValue);

            // Build the cache on first use
            if (matchingFkValuesCache is null)
            {
                matchingFkValuesCache = [];
                foreach (var relatedHandle in relatedFile.EnumerateRowHandles())
                {
                    // Apply inner predicate if present
                    if (innerPredicate is not null && !innerPredicate(relatedFile, relatedHandle))
                        continue;

                    var fkValue = relatedFile.ReadField<int>(relatedHandle, fkFieldIndex);
                    if (fkValue != 0)
                        matchingFkValuesCache.Add(fkValue);
                }
            }

            return matchingFkValuesCache.Contains(principalKeyInt);
        };
    }

    // ── Helpers ──

    private static int ResolveFieldIndex(Db2FieldAccessExpression fieldAccess, Db2TableSchema schema)
    {
        // If the field index was already resolved, use it
        if (fieldAccess.FieldIndex >= 0)
            return fieldAccess.FieldIndex;

        // Resolve by name from the schema
        if (schema.TryGetFieldCaseInsensitive(fieldAccess.Field.Name, out var field))
            return field.ColumnStartIndex;

        throw new InvalidOperationException($"Field '{fieldAccess.Field.Name}' not found in schema '{schema.TableName}'.");
    }

    private static object? ReadFieldBoxed(IDb2File file, RowHandle handle, int fieldIndex, Type clrType)
    {
        // Use a type-dispatch to call the generic ReadField<T> method.
        // Performance note: for hot paths, this could be optimized with cached delegates.
        if (clrType == typeof(int)) return file.ReadField<int>(handle, fieldIndex);
        if (clrType == typeof(uint)) return file.ReadField<uint>(handle, fieldIndex);
        if (clrType == typeof(long)) return file.ReadField<long>(handle, fieldIndex);
        if (clrType == typeof(ulong)) return file.ReadField<ulong>(handle, fieldIndex);
        if (clrType == typeof(short)) return file.ReadField<short>(handle, fieldIndex);
        if (clrType == typeof(ushort)) return file.ReadField<ushort>(handle, fieldIndex);
        if (clrType == typeof(byte)) return file.ReadField<byte>(handle, fieldIndex);
        if (clrType == typeof(sbyte)) return file.ReadField<sbyte>(handle, fieldIndex);
        if (clrType == typeof(float)) return file.ReadField<float>(handle, fieldIndex);
        if (clrType == typeof(double)) return file.ReadField<double>(handle, fieldIndex);
        if (clrType == typeof(bool)) return file.ReadField<bool>(handle, fieldIndex);
        if (clrType == typeof(string)) return file.ReadField<string>(handle, fieldIndex);

        throw new NotSupportedException($"Unsupported field CLR type: {clrType.FullName}");
    }

    private static int CompareValues(object? left, object? right)
    {
        if (left is null && right is null) return 0;
        if (left is null) return -1;
        if (right is null) return 1;

        if (left is IComparable comparable)
            return comparable.CompareTo(right);

        throw new NotSupportedException($"Cannot compare values of type {left.GetType().Name}.");
    }

    private static IEnumerable<RowHandle> ApplyOrdering(
        IEnumerable<RowHandle> handles,
        IDb2File file,
        Db2TableSchema schema,
        IReadOnlyList<Db2OrderingExpression> orderings)
    {
        // Materialize for sorting
        var list = handles.ToList();

        if (orderings.Count == 0)
            return list;

        // Build a comparer from all orderings
        list.Sort((a, b) =>
        {
            for (var i = 0; i < orderings.Count; i++)
            {
                var ordering = orderings[i];
                var fieldIndex = ResolveFieldIndex(ordering.Field, schema);
                var fieldClrType = ordering.Field.Type;
                var valueA = ReadFieldBoxed(file, a, fieldIndex, fieldClrType);
                var valueB = ReadFieldBoxed(file, b, fieldIndex, fieldClrType);

                var cmp = CompareValues(valueA, valueB);
                if (!ordering.Ascending) cmp = -cmp;
                if (cmp != 0) return cmp;
            }
            return 0;
        });

        return list;
    }
}
