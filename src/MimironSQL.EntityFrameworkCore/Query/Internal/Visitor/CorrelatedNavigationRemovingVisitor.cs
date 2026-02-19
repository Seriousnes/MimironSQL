using System.Linq.Expressions;
using System.Reflection;

using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Query;

namespace MimironSQL.EntityFrameworkCore.Query.Internal.Visitor;

internal sealed class CorrelatedNavigationRemovingVisitor(ParameterExpression queryContextParameter) : ExpressionVisitor
{
    private readonly ParameterExpression _queryContextParameter = queryContextParameter;

    protected override Expression VisitMethodCall(MethodCallExpression node)
    {
        // Detect EF Core navigation expansion artifacts of the form:
        //   EntityQueryRoot<TInner>().Where(inner => inner.FK == outerKey).Any(inner => ...)
        //   EntityQueryRoot<TInner>().Where(inner => inner.FK == outerKey).Count()
        // and rewrite them into cached set-based evaluation which does not contain EntityQueryRootExpression.

        if (TryRewriteCorrelatedAny(node, out var rewrittenAny))
            return rewrittenAny;

        if (TryRewriteCorrelatedCount(node, out var rewrittenCount))
            return rewrittenCount;

        return base.VisitMethodCall(node);
    }

    private bool TryRewriteCorrelatedAny(MethodCallExpression node, out Expression rewritten)
    {
        rewritten = null!;

        if (node.Method.Name != nameof(Queryable.Any) || node.Method.DeclaringType != typeof(Queryable))
            return false;

        if (node.Arguments.Count is not (1 or 2))
            return false;

        var source = node.Arguments[0];
        var dependentPredicate = node.Arguments.Count == 2 ? StripQuote(node.Arguments[1]) as LambdaExpression : null;

        if (!TryGetCorrelatedWhere(source, out var innerClrType, out var innerKeyName, out var outerKeyExpression))
            return false;

        var outerKey = Visit(outerKeyExpression);
        var outerKeyType = outerKey.Type;

        Func<QueryContext, object, bool>? compiledDependent = null;
        if (dependentPredicate is not null)
            compiledDependent = CompileInnerPredicate(innerClrType, dependentPredicate);

        var method = CorrelatedNavigationEvaluator.CorrelatedAnyMethodInfo.MakeGenericMethod(innerClrType, outerKeyType);
        rewritten = Expression.Call(
            method,
            _queryContextParameter,
            outerKey,
            Expression.Constant(innerKeyName),
            compiledDependent is null
                ? Expression.Constant(null, typeof(Func<QueryContext, object, bool>))
                : Expression.Constant(compiledDependent, typeof(Func<QueryContext, object, bool>)));

        return true;
    }

    private bool TryRewriteCorrelatedCount(MethodCallExpression node, out Expression rewritten)
    {
        rewritten = null!;

        if (node.Method.Name != nameof(Queryable.Count) || node.Method.DeclaringType != typeof(Queryable))
            return false;

        if (node.Arguments.Count is not (1 or 2))
            return false;

        var source = node.Arguments[0];
        var dependentPredicate = node.Arguments.Count == 2 ? StripQuote(node.Arguments[1]) as LambdaExpression : null;

        if (!TryGetCorrelatedWhere(source, out var innerClrType, out var innerKeyName, out var outerKeyExpression))
            return false;

        var outerKey = Visit(outerKeyExpression);
        var outerKeyType = outerKey.Type;

        Func<QueryContext, object, bool>? compiledDependent = null;
        if (dependentPredicate is not null)
            compiledDependent = CompileInnerPredicate(innerClrType, dependentPredicate);

        var method = CorrelatedNavigationEvaluator.CorrelatedCountMethodInfo.MakeGenericMethod(innerClrType, outerKeyType);
        rewritten = Expression.Call(
            method,
            _queryContextParameter,
            outerKey,
            Expression.Constant(innerKeyName),
            compiledDependent is null
                ? Expression.Constant(null, typeof(Func<QueryContext, object, bool>))
                : Expression.Constant(compiledDependent, typeof(Func<QueryContext, object, bool>)));

        return true;
    }

    private static Expression StripConvert(Expression expression)
    {
        while (expression is UnaryExpression { NodeType: ExpressionType.Convert or ExpressionType.ConvertChecked } u)
            expression = u.Operand;
        return expression;
    }

    private static Expression? StripQuote(Expression expression)
        => expression is UnaryExpression { NodeType: ExpressionType.Quote } u ? u.Operand : expression;

    private static bool TryGetCorrelatedWhere(
        Expression source,
        out Type innerClrType,
        out string innerKeyMemberName,
        out Expression outerKeyExpression)
    {
        innerClrType = null!;
        innerKeyMemberName = null!;
        outerKeyExpression = null!;

        // We only support Where(correlation) immediately before Any/Count.
        if (source is not MethodCallExpression { Method.Name: nameof(Queryable.Where), Method.DeclaringType: { } decl } whereCall
            || decl != typeof(Queryable)
            || whereCall.Arguments.Count != 2)
        {
            return false;
        }

        var whereSource = whereCall.Arguments[0];
        var wherePredicate = StripQuote(whereCall.Arguments[1]) as LambdaExpression;
        if (wherePredicate is null || wherePredicate.Parameters.Count != 1)
            return false;

        innerClrType = wherePredicate.Parameters[0].Type;

        // We expect correlation: inner => inner.FK == outerKey
        if (!TryExtractKeyEquality(wherePredicate, out innerKeyMemberName, out outerKeyExpression))
            return false;

        // Ensure this came from EF Core query root (best-effort): contains EntityQueryRootExpression in the source.
        if (!EntityQueryRootSearchVisitor.ContainsEntityQueryRoot(whereSource))
            return false;

        return true;
    }

    private static bool TryExtractKeyEquality(LambdaExpression correlationPredicate, out string innerMemberName, out Expression outerKeyExpression)
    {
        innerMemberName = null!;
        outerKeyExpression = null!;

        var innerParam = correlationPredicate.Parameters[0];
        var candidates = new List<(Expression Left, Expression Right)>();
        CollectEqualityCandidates(correlationPredicate.Body, candidates);

        foreach (var (candidateLeft, candidateRight) in candidates)
        {
            var left = StripConvert(candidateLeft);
            var right = StripConvert(candidateRight);

            if (TryGetMemberNameOnParameter(left, innerParam, out var innerName) && !ParameterSearchVisitor.Contains(right, innerParam))
            {
                innerMemberName = innerName;
                outerKeyExpression = right;
                return true;
            }

            if (TryGetMemberNameOnParameter(right, innerParam, out innerName) && !ParameterSearchVisitor.Contains(left, innerParam))
            {
                innerMemberName = innerName;
                outerKeyExpression = left;
                return true;
            }
        }

        return false;
    }

    private static void CollectEqualityCandidates(Expression expression, List<(Expression Left, Expression Right)> equalities)
    {
        if (expression is BinaryExpression { NodeType: ExpressionType.Equal } b)
        {
            equalities.Add((b.Left, b.Right));
            CollectEqualityCandidates(b.Left, equalities);
            CollectEqualityCandidates(b.Right, equalities);
            return;
        }

        // EF Core sometimes produces object.Equals(Convert(x, object), Convert(y, object))
        // instead of a BinaryExpression Equal.
        if (expression is MethodCallExpression m && TryGetEqualsOperands(m, out var left, out var right))
        {
            equalities.Add((left, right));
            CollectEqualityCandidates(left, equalities);
            CollectEqualityCandidates(right, equalities);
            return;
        }

        if (expression is BinaryExpression { NodeType: ExpressionType.AndAlso or ExpressionType.OrElse } bb)
        {
            CollectEqualityCandidates(bb.Left, equalities);
            CollectEqualityCandidates(bb.Right, equalities);
            return;
        }

        if (expression is UnaryExpression u)
        {
            CollectEqualityCandidates(u.Operand, equalities);
            return;
        }
    }

    private static bool TryGetEqualsOperands(MethodCallExpression node, out Expression left, out Expression right)
    {
        left = null!;
        right = null!;

        if (node.Method.Name != nameof(object.Equals))
            return false;

        // Static: bool object.Equals(object? objA, object? objB)
        if (node.Object is null && node.Arguments.Count == 2)
        {
            left = node.Arguments[0];
            right = node.Arguments[1];
            return true;
        }

        // Instance: bool x.Equals(object? obj)
        if (node.Object is not null && node.Arguments.Count == 1)
        {
            left = node.Object;
            right = node.Arguments[0];
            return true;
        }

        return false;
    }

    private static bool TryGetMemberNameOnParameter(Expression expression, ParameterExpression parameter, out string name)
    {
        // Supports: inner => inner.FK or inner => EF.Property<T>(inner, "FK")
        if (expression is MemberExpression { Expression: var inst, Member: PropertyInfo p } && inst is not null && StripConvert(inst) == parameter)
        {
            name = p.Name;
            return true;
        }

        if (expression is MethodCallExpression { Method.Name: nameof(EF.Property), Method.DeclaringType: { } decl, Arguments: [var inst2, ConstantExpression { Value: string s }] }
            && decl == typeof(EF)
            && StripConvert(inst2) == parameter)
        {
            name = s;
            return true;
        }

        name = string.Empty;
        return false;
    }

    private Func<QueryContext, object, bool> CompileInnerPredicate(Type innerClrType, LambdaExpression predicate)
    {
        var qc = _queryContextParameter;
        var innerParam = Expression.Parameter(innerClrType, "inner");
        var replaced = new ParameterReplaceVisitor(predicate.Parameters[0], innerParam).Visit(predicate.Body);

        // Reuse the same rewrite rules as outer predicates so query parameters / EF.Property are valid.
        replaced = new QueryParameterRemovingVisitor(qc).Visit(replaced!);
        replaced = new EfPropertyRemovingVisitor(qc).Visit(replaced!);

        var boolBody = replaced!.Type == typeof(bool) ? replaced! : Expression.Convert(replaced!, typeof(bool));

        var typedDelegateType = typeof(Func<,,>).MakeGenericType(typeof(QueryContext), innerClrType, typeof(bool));
        var typedLambda = Expression.Lambda(typedDelegateType, boolBody, qc, innerParam);

        object typedPredicate;
        try
        {
            typedPredicate = typedLambda.Compile();
        }
        catch (ArgumentException ex)
        {
            throw new NotSupportedException(
                "MimironDb2 failed to compile a dependent predicate while rewriting correlated navigations. "
                + $"InnerClrType='{innerClrType.FullName}'. OriginalBody='{predicate.Body}'. RewrittenBody='{replaced}'.",
                ex);
        }

        var boxedInner = Expression.Parameter(typeof(object), "inner");
        var invoke = Expression.Invoke(
            Expression.Constant(typedPredicate, typedDelegateType),
            qc,
            Expression.Convert(boxedInner, innerClrType));

        return Expression.Lambda<Func<QueryContext, object, bool>>(invoke, qc, boxedInner).Compile();
    }
}
