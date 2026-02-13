using System.Linq.Expressions;

using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Storage;

namespace MimironSQL.EntityFrameworkCore.Db2.Query.Expressions;

/// <summary>
/// Represents a DB2 table query — analogous to InMemory's <c>InMemoryQueryExpression</c>.
/// Accumulates filters, projections, ordering, and limits as LINQ operators are translated.
/// </summary>
internal sealed class Db2QueryExpression : Expression
{
    private readonly Dictionary<ProjectionMember, Expression> _projectionMapping = [];
    private List<Db2ProjectionExpression>? _clientProjections;
    private readonly List<Db2OrderingExpression> _orderings = [];

    public Db2QueryExpression(IEntityType entityType)
    {
        EntityType = entityType;
        TableName = entityType.GetTableName() ?? entityType.ClrType.Name;

        // Initialize default "select *" projection mapping:
        // Map the root ProjectionMember to an EntityProjectionExpression-like sentinel.
        // The actual field-level projections are resolved during compilation.
        var entityProjection = new Db2EntityProjectionExpression(entityType);
        _projectionMapping[new ProjectionMember()] = entityProjection;
    }

    // ── Source ──
    public IEntityType EntityType { get; }
    public string TableName { get; }

    // ── Filter ──
    public Db2FilterExpression? Filter { get; private set; }

    // ── Pagination ──
    public int? Limit { get; private set; }
    public string? LimitParameterName { get; private set; }
    public int? Offset { get; private set; }
    public string? OffsetParameterName { get; private set; }

    // ── Ordering ──
    public IReadOnlyList<Db2OrderingExpression> Orderings => _orderings;

    // ── Projection ──
    public IReadOnlyDictionary<ProjectionMember, Expression> ProjectionMapping => _projectionMapping;
    public IReadOnlyList<Db2ProjectionExpression>? ClientProjections => _clientProjections;

    // ── Expression overrides ──
    public override Type Type => typeof(IEnumerable<ValueBuffer>);
    public override ExpressionType NodeType => ExpressionType.Extension;
    protected override Expression VisitChildren(ExpressionVisitor visitor) => this;

    // ── Mutation methods ──

    public void ApplyFilter(Db2FilterExpression filter)
    {
        Filter = Filter is null
            ? filter
            : new Db2AndFilterExpression(Filter, filter);
    }

    public void ApplyLimit(int limit)
    {
        Limit = limit;
        LimitParameterName = null;
    }

    public void ApplyLimitParameter(string parameterName)
    {
        Limit = null;
        LimitParameterName = parameterName;
    }

    public void ApplyOffset(int offset)
    {
        Offset = offset;
        OffsetParameterName = null;
    }

    public void ApplyOffsetParameter(string parameterName)
    {
        Offset = null;
        OffsetParameterName = parameterName;
    }

    public void ApplyOrdering(Db2OrderingExpression ordering)
    {
        _orderings.Add(ordering);
    }

    public void ClearOrderings()
    {
        _orderings.Clear();
    }

    /// <summary>
    /// Replaces the projection mapping with the given expression for the root member.
    /// Used by TranslateSelect to update the shaper.
    /// </summary>
    public void ReplaceProjectionMapping(ProjectionMember projectionMember, Expression expression)
    {
        _projectionMapping[projectionMember] = expression;
    }

    /// <summary>
    /// Gets the mapped expression for a given projection member.
    /// </summary>
    public Expression GetMappedProjection(ProjectionMember projectionMember)
        => _projectionMapping.TryGetValue(projectionMember, out var expression)
            ? expression
            : throw new InvalidOperationException($"Projection member '{projectionMember}' not found.");

    /// <summary>
    /// Finalizes projections. After this call, <see cref="ClientProjections"/> is populated
    /// and the projection mapping is cleared.
    /// </summary>
    public void ApplyProjection()
    {
        // For now, projections are resolved at compilation time by reading all entity properties.
        // This is a simplified implementation; full column-pruning is a later phase.
        _clientProjections ??= [];
    }

    /// <summary>
    /// Adds a projection and returns its index.
    /// </summary>
    public int AddToProjection(Db2ProjectionExpression projection)
    {
        _clientProjections ??= [];
        _clientProjections.Add(projection);
        return _clientProjections.Count - 1;
    }

    public override string ToString()
    {
        var parts = new List<string> { $"FROM {TableName}" };

        if (Filter is not null)
            parts.Add($"WHERE {Filter}");
        if (_orderings.Count > 0)
            parts.Add($"ORDER BY {string.Join(", ", _orderings)}");
        if (Offset.HasValue)
            parts.Add($"OFFSET {Offset}");
        if (Limit.HasValue)
            parts.Add($"LIMIT {Limit}");

        return string.Join(" ", parts);
    }
}
