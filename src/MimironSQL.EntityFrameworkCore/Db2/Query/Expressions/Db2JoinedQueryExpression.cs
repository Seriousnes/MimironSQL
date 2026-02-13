using System.Linq.Expressions;

using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Storage;

namespace MimironSQL.EntityFrameworkCore.Db2.Query.Expressions;

/// <summary>
/// Represents a query that joins two DB2 tables.
/// Used for navigation property access patterns like <c>.Include(x => x.Map).Where(x => x.Map.Directory == "foo")</c>.
/// </summary>
internal sealed class Db2JoinedQueryExpression(
    Db2QueryExpression outer,
    IEntityType innerEntityType,
    string innerTableName,
    string outerKeyColumn,
    string innerKeyColumn,
    bool isLeftJoin) : Expression
{
    private readonly List<Db2OrderingExpression> _orderings = [];

    /// <summary>
    /// The outer (principal) query expression.
    /// </summary>
    public Db2QueryExpression Outer { get; } = outer;

    /// <summary>
    /// The entity type of the inner (related) table.
    /// </summary>
    public IEntityType InnerEntityType { get; } = innerEntityType;

    /// <summary>
    /// The table name of the inner (related) table.
    /// </summary>
    public string InnerTableName { get; } = innerTableName;

    /// <summary>
    /// The column name in the outer table used for the join (typically the FK).
    /// </summary>
    public string OuterKeyColumn { get; } = outerKeyColumn;

    /// <summary>
    /// The column name in the inner table used for the join (typically the PK).
    /// </summary>
    public string InnerKeyColumn { get; } = innerKeyColumn;

    /// <summary>
    /// Whether this is a LEFT JOIN (nullable relationship) or INNER JOIN.
    /// </summary>
    public bool IsLeftJoin { get; } = isLeftJoin;

    // ── Filter ──
    public Db2FilterExpression? JoinedFilter { get; private set; }

    // ── Pagination ──
    public int? Limit { get; private set; }
    public string? LimitParameterName { get; private set; }
    public int? Offset { get; private set; }
    public string? OffsetParameterName { get; private set; }

    // ── Ordering ──
    public IReadOnlyList<Db2OrderingExpression> Orderings => _orderings;

    // ── Expression overrides ──
    public override Type Type => typeof(IEnumerable<ValueBuffer>);
    public override ExpressionType NodeType => ExpressionType.Extension;
    protected override Expression VisitChildren(ExpressionVisitor visitor) => this;

    // ── Mutation methods ──

    public void ApplyFilter(Db2FilterExpression filter)
    {
        JoinedFilter = JoinedFilter is null
            ? filter
            : new Db2AndFilterExpression(JoinedFilter, filter);
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
}
