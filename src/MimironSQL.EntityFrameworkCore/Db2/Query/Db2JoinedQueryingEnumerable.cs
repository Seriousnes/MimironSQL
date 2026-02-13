using System.Collections;
using System.Linq.Expressions;

using Microsoft.EntityFrameworkCore.Query;

using MimironSQL.EntityFrameworkCore.Db2.Query.Expressions;
using MimironSQL.EntityFrameworkCore.Db2.Schema;
using MimironSQL.EntityFrameworkCore.Storage;
using MimironSQL.Formats;

namespace MimironSQL.EntityFrameworkCore.Db2.Query;

/// <summary>
/// Runtime enumerable that executes a joined DB2 query.
/// Joins two tables by FK-PK relationship and applies filters on the combined result.
/// </summary>
internal sealed class Db2JoinedQueryingEnumerable<TOuter, TInner, TResult>(
    QueryContext queryContext,
    Db2JoinedQueryExecutionPlan plan,
    IDb2File outerFile,
    Db2TableSchema outerSchema,
    IDb2File innerFile,
#pragma warning disable CS9113 // Parameter is unread.
    Db2TableSchema innerSchema, // TODO: implement support for using inner schema in joined filters
#pragma warning restore CS9113 // Parameter is unread.
    Func<QueryContext, IDb2File, RowHandle, TOuter> outerShaper,
#pragma warning disable CS9113 // Parameter is unread.
    Func<QueryContext, IDb2File, RowHandle, TInner> innerShaper, // TODO: determine if needed, remove if not
#pragma warning restore CS9113 // Parameter is unread.
    Dictionary<int, (TInner Entity, RowHandle Handle)> innerLookup,
    int outerKeyIndex,
    Func<IDb2File, RowHandle, IDb2File, RowHandle?, bool>? joinedFilter,
    Func<TOuter, TInner?, TResult> resultProjection)
    : IEnumerable<TResult>, IAsyncEnumerable<TResult>
    where TOuter : class
    where TInner : class
{
    public IEnumerator<TResult> GetEnumerator()
        => new Enumerator(
            queryContext,
            plan,
            outerFile,
            outerSchema,
            innerFile,
            innerLookup,
            outerKeyIndex,
            joinedFilter,
            outerShaper,
            resultProjection);

    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

    public IAsyncEnumerator<TResult> GetAsyncEnumerator(CancellationToken cancellationToken = default)
        => new AsyncEnumerator(GetEnumerator(), cancellationToken);

    private sealed class Enumerator(
        QueryContext queryContext,
        Db2JoinedQueryExecutionPlan plan,
        IDb2File outerFile,
#pragma warning disable CS9113 // Parameter is unread.
        Db2TableSchema outerSchema, // TODO: Implement for future use
#pragma warning restore CS9113 // Parameter is unread.
        IDb2File innerFile,
        Dictionary<int, (TInner Entity, RowHandle Handle)> innerLookup,
        int outerKeyIndex,
        Func<IDb2File, RowHandle, IDb2File, RowHandle?, bool>? joinedFilter,
        Func<QueryContext, IDb2File, RowHandle, TOuter> outerShaper,
        Func<TOuter, TInner?, TResult> resultProjection) : IEnumerator<TResult>
    {
        private IEnumerator<RowHandle>? _outerEnumerator;
        private bool _initialized;
        private int _skipped;
        private int _returned;

        public TResult Current { get; private set; } = default!;
        object? IEnumerator.Current => Current;

        public bool MoveNext()
        {
            if (!_initialized)
            {
                Initialize();
                _initialized = true;
            }

            if (_outerEnumerator is null)
                return false;

            // Resolve limit
            var limit = plan.Limit
                ?? (plan.LimitParameterName is not null
                    ? Convert.ToInt32(queryContext.Parameters[plan.LimitParameterName])
                    : (int?)null);

            // Resolve offset
            var offset = plan.Offset
                ?? (plan.OffsetParameterName is not null
                    ? Convert.ToInt32(queryContext.Parameters[plan.OffsetParameterName])
                    : (int?)null);

            while (_outerEnumerator.MoveNext())
            {
                var outerHandle = _outerEnumerator.Current;

                // Get the FK value from the outer entity
                var fkValue = outerFile.ReadField<int>(outerHandle, outerKeyIndex);

                // Look up the inner entity
                RowHandle? innerHandle = null;
                TInner? innerEntity = default;

                if (fkValue != 0 && innerLookup.TryGetValue(fkValue, out var innerData))
                {
                    innerHandle = innerData.Handle;
                    innerEntity = innerData.Entity;
                }
                else if (!plan.IsLeftJoin)
                {
                    // INNER JOIN: skip rows without a match
                    continue;
                }

                // Apply joined filter if present
                if (joinedFilter is not null && !joinedFilter(outerFile, outerHandle, innerFile, innerHandle))
                {
                    continue;
                }

                // Apply offset
                if (offset.HasValue && _skipped < offset.Value)
                {
                    _skipped++;
                    continue;
                }

                // Apply limit
                if (limit.HasValue && _returned >= limit.Value)
                {
                    return false;
                }

                // Shape the outer entity
                var outerEntity = outerShaper(queryContext, outerFile, outerHandle);

                // Set navigation property if both entities present
                if (innerEntity is not null && outerEntity is not null)
                {
                    SetNavigationProperty(outerEntity, innerEntity);
                }

                // Apply the result projection to produce the final result
                Current = resultProjection(outerEntity!, innerEntity);
                _returned++;
                return true;
            }

            Current = default!;
            return false;
        }

        private void Initialize()
        {
            _outerEnumerator = outerFile.EnumerateRowHandles().GetEnumerator();
        }

        private static void SetNavigationProperty(TOuter outer, TInner inner)
        {
            // Find the navigation property on TOuter that references TInner
            var innerType = typeof(TInner);
            var outerType = typeof(TOuter);

            foreach (var prop in outerType.GetProperties())
            {
                if (prop.PropertyType == innerType && prop.CanWrite)
                {
                    prop.SetValue(outer, inner);
                    break;
                }
            }
        }

        public void Reset() => throw new NotSupportedException();

        public void Dispose()
        {
            _outerEnumerator?.Dispose();
        }
    }

    private sealed class AsyncEnumerator(
        IEnumerator<TResult> inner,
        CancellationToken cancellationToken) : IAsyncEnumerator<TResult>
    {
        public TResult Current => inner.Current;

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
}

/// <summary>
/// Static helper for compiling joined filters.
/// </summary>
internal static class Db2JoinedQueryingEnumerable
{
    /// <summary>
    /// Compiles a joined filter expression into a runtime predicate.
    /// </summary>
    public static Func<IDb2File, RowHandle, IDb2File, RowHandle?, bool> CompileJoinedFilter(
        Db2FilterExpression filter,
        Db2TableSchema outerSchema,
        Db2TableSchema innerSchema,
        QueryContext queryContext,
        IMimironDb2Store store)
    {
        return filter switch
        {
            Db2JoinedComparisonFilterExpression comparison => CompileJoinedComparison(comparison, innerSchema, queryContext),
            Db2JoinedStringMatchFilterExpression stringMatch => CompileJoinedStringMatch(stringMatch, innerSchema, queryContext),
            Db2JoinedNullCheckFilterExpression nullCheck => CompileJoinedNullCheck(nullCheck, innerSchema),
            Db2ComparisonFilterExpression comparison => CompileOuterComparison(comparison, outerSchema, queryContext),
            Db2StringMatchFilterExpression stringMatch => CompileOuterStringMatch(stringMatch, outerSchema, queryContext),
            Db2NullCheckFilterExpression nullCheck => CompileOuterNullCheck(nullCheck, outerSchema),
            Db2AndFilterExpression and => CompileJoinedAnd(and, outerSchema, innerSchema, queryContext, store),
            Db2OrFilterExpression or => CompileJoinedOr(or, outerSchema, innerSchema, queryContext, store),
            Db2NotFilterExpression not => CompileJoinedNot(not, outerSchema, innerSchema, queryContext, store),
            _ => throw new NotSupportedException($"Unsupported joined filter expression type: {filter.GetType().Name}"),
        };
    }

    private static Func<IDb2File, RowHandle, IDb2File, RowHandle?, bool> CompileJoinedComparison(
        Db2JoinedComparisonFilterExpression comparison,
        Db2TableSchema innerSchema,
        QueryContext queryContext)
    {
        var fieldIndex = ResolveFieldIndex(comparison.Field.Field.Name, innerSchema);
        var fieldClrType = comparison.Field.Type;

        var value = comparison.Value is Db2RuntimeParameter param
            ? queryContext.Parameters[param.Name]
            : comparison.Value;

        return comparison.ComparisonKind switch
        {
            ExpressionType.Equal => (outerFile, outerHandle, innerFile, innerHandle) =>
                innerHandle.HasValue && Equals(ReadFieldBoxed(innerFile, innerHandle.Value, fieldIndex, fieldClrType), value),
            ExpressionType.NotEqual => (outerFile, outerHandle, innerFile, innerHandle) =>
                innerHandle.HasValue && !Equals(ReadFieldBoxed(innerFile, innerHandle.Value, fieldIndex, fieldClrType), value),
            ExpressionType.GreaterThan => (outerFile, outerHandle, innerFile, innerHandle) =>
                innerHandle.HasValue && CompareValues(ReadFieldBoxed(innerFile, innerHandle.Value, fieldIndex, fieldClrType), value) > 0,
            ExpressionType.GreaterThanOrEqual => (outerFile, outerHandle, innerFile, innerHandle) =>
                innerHandle.HasValue && CompareValues(ReadFieldBoxed(innerFile, innerHandle.Value, fieldIndex, fieldClrType), value) >= 0,
            ExpressionType.LessThan => (outerFile, outerHandle, innerFile, innerHandle) =>
                innerHandle.HasValue && CompareValues(ReadFieldBoxed(innerFile, innerHandle.Value, fieldIndex, fieldClrType), value) < 0,
            ExpressionType.LessThanOrEqual => (outerFile, outerHandle, innerFile, innerHandle) =>
                innerHandle.HasValue && CompareValues(ReadFieldBoxed(innerFile, innerHandle.Value, fieldIndex, fieldClrType), value) <= 0,
            _ => throw new NotSupportedException($"Unsupported comparison kind: {comparison.ComparisonKind}"),
        };
    }

    private static Func<IDb2File, RowHandle, IDb2File, RowHandle?, bool> CompileJoinedStringMatch(
        Db2JoinedStringMatchFilterExpression stringMatch,
        Db2TableSchema innerSchema,
        QueryContext queryContext)
    {
        var fieldIndex = ResolveFieldIndex(stringMatch.Field.Field.Name, innerSchema);
        var patternObj = stringMatch.PatternParameterName is not null
            ? queryContext.Parameters[stringMatch.PatternParameterName]
            : stringMatch.Pattern;

        var pattern = patternObj switch
        {
            null => null,
            string s => s,
            char c => c.ToString(),
            _ => patternObj.ToString(),
        };

        return stringMatch.MatchKind switch
        {
            Db2StringMatchKind.Contains => (outerFile, outerHandle, innerFile, innerHandle) =>
            {
                if (!innerHandle.HasValue) return false;
                var val = innerFile.ReadField<string>(innerHandle.Value, fieldIndex);
                return val is not null && pattern is not null && val.Contains(pattern, StringComparison.OrdinalIgnoreCase);
            },
            Db2StringMatchKind.StartsWith => (outerFile, outerHandle, innerFile, innerHandle) =>
            {
                if (!innerHandle.HasValue) return false;
                var val = innerFile.ReadField<string>(innerHandle.Value, fieldIndex);
                return val is not null && pattern is not null && val.StartsWith(pattern, StringComparison.OrdinalIgnoreCase);
            },
            Db2StringMatchKind.EndsWith => (outerFile, outerHandle, innerFile, innerHandle) =>
            {
                if (!innerHandle.HasValue) return false;
                var val = innerFile.ReadField<string>(innerHandle.Value, fieldIndex);
                return val is not null && pattern is not null && val.EndsWith(pattern, StringComparison.OrdinalIgnoreCase);
            },
            _ => throw new NotSupportedException($"Unsupported string match kind: {stringMatch.MatchKind}"),
        };
    }

    private static Func<IDb2File, RowHandle, IDb2File, RowHandle?, bool> CompileJoinedNullCheck(
        Db2JoinedNullCheckFilterExpression nullCheck,
        Db2TableSchema innerSchema)
    {
        // For joined null checks, we check if the inner handle exists
        if (nullCheck.IsNotNull)
            return (outerFile, outerHandle, innerFile, innerHandle) => innerHandle.HasValue;
        else
            return (outerFile, outerHandle, innerFile, innerHandle) => !innerHandle.HasValue;
    }

    private static Func<IDb2File, RowHandle, IDb2File, RowHandle?, bool> CompileOuterComparison(
        Db2ComparisonFilterExpression comparison,
        Db2TableSchema outerSchema,
        QueryContext queryContext)
    {
        var fieldIndex = ResolveFieldIndex(comparison.Field.Field.Name, outerSchema);
        var fieldClrType = comparison.Field.Type;

        var value = comparison.Value is Db2RuntimeParameter param
            ? queryContext.Parameters[param.Name]
            : comparison.Value;

        return comparison.ComparisonKind switch
        {
            ExpressionType.Equal => (outerFile, outerHandle, innerFile, innerHandle) =>
                Equals(ReadFieldBoxed(outerFile, outerHandle, fieldIndex, fieldClrType), value),
            ExpressionType.NotEqual => (outerFile, outerHandle, innerFile, innerHandle) =>
                !Equals(ReadFieldBoxed(outerFile, outerHandle, fieldIndex, fieldClrType), value),
            _ => throw new NotSupportedException($"Unsupported comparison kind for outer: {comparison.ComparisonKind}"),
        };
    }

    private static Func<IDb2File, RowHandle, IDb2File, RowHandle?, bool> CompileOuterStringMatch(
        Db2StringMatchFilterExpression stringMatch,
        Db2TableSchema outerSchema,
        QueryContext queryContext)
    {
        var fieldIndex = ResolveFieldIndex(stringMatch.Field.Field.Name, outerSchema);
        var patternObj = stringMatch.PatternParameterName is not null
            ? queryContext.Parameters[stringMatch.PatternParameterName]
            : stringMatch.Pattern;

        var pattern = patternObj switch
        {
            null => null,
            string s => s,
            char c => c.ToString(),
            _ => patternObj.ToString(),
        };

        return stringMatch.MatchKind switch
        {
            Db2StringMatchKind.Contains => (outerFile, outerHandle, innerFile, innerHandle) =>
            {
                var val = outerFile.ReadField<string>(outerHandle, fieldIndex);
                return val is not null && pattern is not null && val.Contains(pattern, StringComparison.OrdinalIgnoreCase);
            },
            Db2StringMatchKind.StartsWith => (outerFile, outerHandle, innerFile, innerHandle) =>
            {
                var val = outerFile.ReadField<string>(outerHandle, fieldIndex);
                return val is not null && pattern is not null && val.StartsWith(pattern, StringComparison.OrdinalIgnoreCase);
            },
            Db2StringMatchKind.EndsWith => (outerFile, outerHandle, innerFile, innerHandle) =>
            {
                var val = outerFile.ReadField<string>(outerHandle, fieldIndex);
                return val is not null && pattern is not null && val.EndsWith(pattern, StringComparison.OrdinalIgnoreCase);
            },
            _ => throw new NotSupportedException($"Unsupported string match kind: {stringMatch.MatchKind}"),
        };
    }

    private static Func<IDb2File, RowHandle, IDb2File, RowHandle?, bool> CompileOuterNullCheck(
        Db2NullCheckFilterExpression nullCheck,
        Db2TableSchema outerSchema)
    {
        var fieldIndex = ResolveFieldIndex(nullCheck.Field.Field.Name, outerSchema);
        var fieldClrType = nullCheck.Field.Type;

        if (nullCheck.IsNotNull)
            return (outerFile, outerHandle, innerFile, innerHandle) =>
                ReadFieldBoxed(outerFile, outerHandle, fieldIndex, fieldClrType) is not null;
        else
            return (outerFile, outerHandle, innerFile, innerHandle) =>
                ReadFieldBoxed(outerFile, outerHandle, fieldIndex, fieldClrType) is null;
    }

    private static Func<IDb2File, RowHandle, IDb2File, RowHandle?, bool> CompileJoinedAnd(
        Db2AndFilterExpression and,
        Db2TableSchema outerSchema,
        Db2TableSchema innerSchema,
        QueryContext queryContext,
        IMimironDb2Store store)
    {
        var left = CompileJoinedFilter(and.Left, outerSchema, innerSchema, queryContext, store);
        var right = CompileJoinedFilter(and.Right, outerSchema, innerSchema, queryContext, store);
        return (outerFile, outerHandle, innerFile, innerHandle) =>
            left(outerFile, outerHandle, innerFile, innerHandle) && right(outerFile, outerHandle, innerFile, innerHandle);
    }

    private static Func<IDb2File, RowHandle, IDb2File, RowHandle?, bool> CompileJoinedOr(
        Db2OrFilterExpression or,
        Db2TableSchema outerSchema,
        Db2TableSchema innerSchema,
        QueryContext queryContext,
        IMimironDb2Store store)
    {
        var left = CompileJoinedFilter(or.Left, outerSchema, innerSchema, queryContext, store);
        var right = CompileJoinedFilter(or.Right, outerSchema, innerSchema, queryContext, store);
        return (outerFile, outerHandle, innerFile, innerHandle) =>
            left(outerFile, outerHandle, innerFile, innerHandle) || right(outerFile, outerHandle, innerFile, innerHandle);
    }

    private static Func<IDb2File, RowHandle, IDb2File, RowHandle?, bool> CompileJoinedNot(
        Db2NotFilterExpression not,
        Db2TableSchema outerSchema,
        Db2TableSchema innerSchema,
        QueryContext queryContext,
        IMimironDb2Store store)
    {
        var inner = CompileJoinedFilter(not.Inner, outerSchema, innerSchema, queryContext, store);
        return (outerFile, outerHandle, innerFile, innerHandle) =>
            !inner(outerFile, outerHandle, innerFile, innerHandle);
    }

    private static int ResolveFieldIndex(string fieldName, Db2TableSchema schema)
    {
        if (schema.TryGetFieldCaseInsensitive(fieldName, out var field))
            return field.ColumnStartIndex;

        throw new InvalidOperationException($"Field '{fieldName}' not found in schema '{schema.TableName}'.");
    }

    private static object? ReadFieldBoxed(IDb2File file, RowHandle handle, int fieldIndex, Type clrType)
    {
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
}
