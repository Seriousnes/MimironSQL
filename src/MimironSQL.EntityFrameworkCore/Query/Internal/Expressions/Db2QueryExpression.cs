using System.Linq.Expressions;

using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Storage;

namespace MimironSQL.EntityFrameworkCore.Query.Internal.Expressions;

internal sealed class Db2QueryExpression(IEntityType entityType) : Expression
{
    internal enum Db2TerminalOperator
    {
        None = 0,
        Count = 1,
        Last = 2,
        LastOrDefault = 3,
    }

    public IEntityType EntityType { get; } = entityType ?? throw new ArgumentNullException(nameof(entityType));

    public List<LambdaExpression> Predicates { get; } = [];

    // Ordering: list of (keySelector lambda, ascending flag) pairs.
    public List<(LambdaExpression KeySelector, bool Ascending)> Orderings { get; } = [];

    // Represents Queryable.Join/LeftJoin/RightJoin without introducing provider-specific join enums/types.
    // JoinOperator values are expected to be nameof(Queryable.Join)/nameof(Queryable.LeftJoin)/nameof(Queryable.RightJoin).
    public List<(string JoinOperator, Db2QueryExpression Inner, LambdaExpression OuterKeySelector, LambdaExpression InnerKeySelector)> Joins { get; } = [];

    public Expression? Limit { get; private set; }

    // Skip/Offset.
    public Expression? Offset { get; private set; }

    public Db2TerminalOperator TerminalOperator { get; private set; } = Db2TerminalOperator.None;

    public bool NegateScalarResult { get; private set; }

    public void ApplyPredicate(LambdaExpression predicate)
    {
        ArgumentNullException.ThrowIfNull(predicate);
        Predicates.Add(predicate);
    }

    public void ApplyOrdering(LambdaExpression keySelector, bool ascending)
    {
        ArgumentNullException.ThrowIfNull(keySelector);
        Orderings.Add((keySelector, ascending));
    }

    public void ApplyJoin(
        string joinOperator,
        Db2QueryExpression inner,
        LambdaExpression outerKeySelector,
        LambdaExpression innerKeySelector)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(joinOperator);
        ArgumentNullException.ThrowIfNull(inner);
        ArgumentNullException.ThrowIfNull(outerKeySelector);
        ArgumentNullException.ThrowIfNull(innerKeySelector);

        Joins.Add((joinOperator, inner, outerKeySelector, innerKeySelector));
    }

    public void ApplyLimit(Expression limit)
    {
        ArgumentNullException.ThrowIfNull(limit);

        if (limit.Type != typeof(int))
        {
            throw new ArgumentException("Limit expression must be of type int.", nameof(limit));
        }

        if (Limit is null)
        {
            Limit = limit;
            return;
        }

        // Multiple Take() calls should apply the most restrictive limit.
        Limit = Expression.Call(
            typeof(Math).GetMethod(nameof(Math.Min), [typeof(int), typeof(int)])!,
            Limit,
            limit);
    }

    public void ApplyOffset(Expression offset)
    {
        ArgumentNullException.ThrowIfNull(offset);

        if (offset.Type != typeof(int))
        {
            throw new ArgumentException("Offset expression must be of type int.", nameof(offset));
        }

        Offset = Offset is null
            ? offset
            : Expression.Add(Offset, offset);
    }

    public void ApplyTerminalOperator(Db2TerminalOperator terminalOperator)
    {
        TerminalOperator = terminalOperator;
    }

    public void ApplyScalarNegation() => NegateScalarResult = !NegateScalarResult;

    public override Type Type => typeof(IEnumerable<ValueBuffer>);

    public override ExpressionType NodeType => ExpressionType.Extension;

    protected override Expression VisitChildren(ExpressionVisitor visitor) => this;
}
