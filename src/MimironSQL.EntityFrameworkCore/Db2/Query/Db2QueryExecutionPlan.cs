using Microsoft.EntityFrameworkCore.Metadata;

using MimironSQL.EntityFrameworkCore.Db2.Query.Expressions;

namespace MimironSQL.EntityFrameworkCore.Db2.Query;

/// <summary>
/// A pre-analyzed execution plan that <see cref="Db2QueryingEnumerable{T}"/> uses at runtime.
/// Separates compilation-time analysis from runtime execution.
/// </summary>
internal sealed class Db2QueryExecutionPlan
{
    public required string TableName { get; init; }
    public required IEntityType EntityType { get; init; }

    /// <summary>
    /// Pre-analyzed execution strategy.
    /// </summary>
    public required Db2ExecutionStrategy Strategy { get; init; }

    /// <summary>
    /// For PK lookups: the IDs to retrieve.
    /// </summary>
    public IReadOnlyList<int>? PrimaryKeyIds { get; init; }

    /// <summary>
    /// For runtime PK lookups: the parameter name whose value is the PK ID or ID collection.
    /// </summary>
    public string? PrimaryKeyParameterName { get; init; }

    /// <summary>
    /// Compiled row-level filter for predicates that map to field reads.
    /// </summary>
    public Db2FilterExpression? Filter { get; init; }

    /// <summary>
    /// Projection info: which fields to read, in what order.
    /// </summary>
    public IReadOnlyList<Db2ProjectionExpression>? Projections { get; init; }

    /// <summary>
    /// Maximum number of rows to return.
    /// </summary>
    public int? Limit { get; init; }

    /// <summary>
    /// Runtime parameter name for limit (when not known at compile time).
    /// </summary>
    public string? LimitParameterName { get; init; }

    /// <summary>
    /// Number of rows to skip.
    /// </summary>
    public int? Offset { get; init; }

    /// <summary>
    /// Runtime parameter name for offset (when not known at compile time).
    /// </summary>
    public string? OffsetParameterName { get; init; }

    /// <summary>
    /// Ordering expressions for in-memory sorting.
    /// </summary>
    public IReadOnlyList<Db2OrderingExpression>? Orderings { get; init; }

    /// <summary>
    /// Creates an execution plan from a <see cref="Db2QueryExpression"/>.
    /// Analyzes the filter tree to determine the optimal execution strategy.
    /// </summary>
    public static Db2QueryExecutionPlan FromQueryExpression(Db2QueryExpression queryExpression)
    {
        var strategy = Db2ExecutionStrategy.FullScan;
        IReadOnlyList<int>? pkIds = null;
        string? pkParameterName = null;

        // Analyze filter for PK-based optimizations
        if (queryExpression.Filter is not null)
        {
            if (TryExtractPrimaryKeyEquality(queryExpression.Filter, out var singleId))
            {
                strategy = Db2ExecutionStrategy.PrimaryKeyLookup;
                pkIds = [singleId];
            }
            else if (TryExtractPrimaryKeyContains(queryExpression.Filter, out var ids))
            {
                strategy = Db2ExecutionStrategy.PrimaryKeyMultiLookup;
                pkIds = ids;
            }
            else if (TryExtractPrimaryKeyParameterEquality(queryExpression.Filter, out var paramName))
            {
                // PK lookup via runtime parameter — resolved at execution time
                strategy = Db2ExecutionStrategy.RuntimePrimaryKeyLookup;
                pkParameterName = paramName;
            }
            // Note: parameter-based Contains (e.g., ids.Contains(x.Id) with a runtime collection)
            // falls through to FullScan with a compiled filter. This ensures correct file-order
            // enumeration and deduplication, unlike PK multi-lookup which yields in parameter order.
        }

        var keepFilter = strategy == Db2ExecutionStrategy.FullScan;

        return new Db2QueryExecutionPlan
        {
            TableName = queryExpression.TableName,
            EntityType = queryExpression.EntityType,
            Strategy = strategy,
            PrimaryKeyIds = pkIds,
            PrimaryKeyParameterName = pkParameterName,
            Filter = keepFilter ? queryExpression.Filter : null,
            Projections = queryExpression.ClientProjections is { Count: > 0 } p ? p : null,
            Limit = queryExpression.Limit,
            LimitParameterName = queryExpression.LimitParameterName,
            Offset = queryExpression.Offset,
            OffsetParameterName = queryExpression.OffsetParameterName,
            Orderings = queryExpression.Orderings is { Count: > 0 } o ? o.ToArray() : null,
        };
    }

    private static bool TryExtractPrimaryKeyEquality(Db2FilterExpression filter, out int id)
    {
        id = 0;

        if (filter is Db2ComparisonFilterExpression { ComparisonKind: Db2ComparisonKind.Equal } comparison
            && comparison.Field.Field.IsId
            && comparison.Value is int intValue)
        {
            id = intValue;
            return true;
        }

        return false;
    }

    private static bool TryExtractPrimaryKeyParameterEquality(Db2FilterExpression filter, out string parameterName)
    {
        parameterName = null!;

        if (filter is Db2ComparisonFilterExpression { ComparisonKind: Db2ComparisonKind.Equal } comparison
            && comparison.Field.Field.IsId
            && comparison.Value is Db2RuntimeParameter param)
        {
            parameterName = param.Name;
            return true;
        }

        return false;
    }

    private static bool TryExtractPrimaryKeyContains(Db2FilterExpression filter, out IReadOnlyList<int> ids)
    {
        ids = [];

        if (filter is Db2ContainsFilterExpression { Values: not null } contains
            && contains.Field.Field.IsId)
        {
            var intIds = new List<int>();
            foreach (var value in contains.Values)
            {
                if (value is int intValue)
                    intIds.Add(intValue);
                else
                    return false;
            }
            ids = intIds;
            return true;
        }

        return false;
    }

    private static bool TryExtractPrimaryKeyParameterContains(Db2FilterExpression filter, out string parameterName)
    {
        parameterName = null!;

        if (filter is Db2ContainsFilterExpression { ValuesParameterName: not null } contains
            && contains.Field.Field.IsId)
        {
            parameterName = contains.ValuesParameterName;
            return true;
        }

        return false;
    }
}

internal enum Db2ExecutionStrategy
{
    /// <summary>Enumerate all rows, apply filter.</summary>
    FullScan,

    /// <summary>Single ID lookup via TryGetRowHandle (constant ID known at compile time).</summary>
    PrimaryKeyLookup,

    /// <summary>Multiple ID lookup via TryGetRowHandle (constant IDs known at compile time).</summary>
    PrimaryKeyMultiLookup,

    /// <summary>Single PK lookup resolved from a runtime parameter.</summary>
    RuntimePrimaryKeyLookup,

    /// <summary>Multi-PK lookup resolved from a runtime parameter collection.</summary>
    RuntimePrimaryKeyMultiLookup,
}
