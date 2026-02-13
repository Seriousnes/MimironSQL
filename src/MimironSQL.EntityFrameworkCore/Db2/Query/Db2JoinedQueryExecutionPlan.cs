using Microsoft.EntityFrameworkCore.Metadata;

using MimironSQL.EntityFrameworkCore.Db2.Query.Expressions;

namespace MimironSQL.EntityFrameworkCore.Db2.Query;

/// <summary>
/// Execution plan for a joined query that combines two DB2 tables.
/// </summary>
internal sealed class Db2JoinedQueryExecutionPlan
{
    /// <summary>
    /// The execution plan for the outer (principal) query.
    /// </summary>
    public required Db2QueryExecutionPlan OuterPlan { get; init; }

    /// <summary>
    /// The entity type of the inner (related) table.
    /// </summary>
    public required IEntityType InnerEntityType { get; init; }

    /// <summary>
    /// The table name of the inner (related) table.
    /// </summary>
    public required string InnerTableName { get; init; }

    /// <summary>
    /// The column name in the outer table used for the join (typically the FK).
    /// </summary>
    public required string OuterKeyColumn { get; init; }

    /// <summary>
    /// The column name in the inner table used for the join (typically the PK).
    /// </summary>
    public required string InnerKeyColumn { get; init; }

    /// <summary>
    /// Whether this is a LEFT JOIN (nullable relationship) or INNER JOIN.
    /// </summary>
    public required bool IsLeftJoin { get; init; }

    /// <summary>
    /// Filter to apply on the joined result (may reference both outer and inner fields).
    /// </summary>
    public Db2FilterExpression? JoinedFilter { get; init; }

    /// <summary>
    /// Maximum number of rows to return.
    /// </summary>
    public int? Limit { get; init; }

    /// <summary>
    /// Runtime parameter name for limit.
    /// </summary>
    public string? LimitParameterName { get; init; }

    /// <summary>
    /// Number of rows to skip.
    /// </summary>
    public int? Offset { get; init; }

    /// <summary>
    /// Runtime parameter name for offset.
    /// </summary>
    public string? OffsetParameterName { get; init; }

    /// <summary>
    /// Creates an execution plan from a <see cref="Db2JoinedQueryExpression"/>.
    /// </summary>
    public static Db2JoinedQueryExecutionPlan FromJoinedQueryExpression(
        Db2JoinedQueryExpression joinedQuery,
        Db2QueryExecutionPlan outerPlan)
    {
        return new Db2JoinedQueryExecutionPlan
        {
            OuterPlan = outerPlan,
            InnerEntityType = joinedQuery.InnerEntityType,
            InnerTableName = joinedQuery.InnerTableName,
            OuterKeyColumn = joinedQuery.OuterKeyColumn,
            InnerKeyColumn = joinedQuery.InnerKeyColumn,
            IsLeftJoin = joinedQuery.IsLeftJoin,
            JoinedFilter = joinedQuery.JoinedFilter,
            Limit = joinedQuery.Limit,
            LimitParameterName = joinedQuery.LimitParameterName,
            Offset = joinedQuery.Offset,
            OffsetParameterName = joinedQuery.OffsetParameterName,
        };
    }
}
