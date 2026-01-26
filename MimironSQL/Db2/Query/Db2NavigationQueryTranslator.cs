using System.Linq.Expressions;
using System.Reflection;

using MimironSQL.Db2.Model;

namespace MimironSQL.Db2.Query;

internal static class Db2NavigationQueryTranslator
{
    public static bool TryTranslateStringPredicate<TEntity>(
        Db2Model model,
        Expression<Func<TEntity, bool>> predicate,
        out Db2NavigationStringPredicatePlan plan)
    {
        ArgumentNullException.ThrowIfNull(model);
        ArgumentNullException.ThrowIfNull(predicate);

        plan = null!;

        if (predicate.Parameters.Count != 1 || predicate.Parameters[0].Type != typeof(TEntity))
            return false;

        var rootParam = predicate.Parameters[0];

        var body = UnwrapConvert(predicate.Body);

        if (!TryParseNavigationStringPredicate(body, rootParam, out var navMember, out var targetMember, out var matchKind, out var needle))
            return false;

        if (!model.TryGetReferenceNavigation(typeof(TEntity), navMember, out var navigation))
            return false;

        var root = model.GetEntityType(typeof(TEntity));
        var target = model.GetEntityType(navigation.TargetClrType);

        plan = new Db2NavigationStringPredicatePlan(
            Join: new Db2NavigationJoinPlan(root, navigation, target),
            TargetStringMember: targetMember,
            MatchKind: matchKind,
            Needle: needle);

        return true;
    }

    public static IReadOnlyList<Db2NavigationMemberAccessPlan> GetNavigationAccesses<TEntity>(
        Db2Model model,
        LambdaExpression lambda)
    {
        ArgumentNullException.ThrowIfNull(model);
        ArgumentNullException.ThrowIfNull(lambda);

        if (lambda.Parameters.Count != 1 || lambda.Parameters[0].Type != typeof(TEntity))
            return [];

        var rootParam = lambda.Parameters[0];
        var accesses = new List<Db2NavigationMemberAccessPlan>();

        void Visit(Expression? expr)
        {
            if (expr is null)
                return;

            expr = UnwrapConvert(expr);

            switch (expr)
            {
                case MemberExpression { Member: PropertyInfo or FieldInfo } member:
                {
                    // 1-hop: x.Nav.Member
                    if (UnwrapConvert(member.Expression) is MemberExpression { Member: PropertyInfo or FieldInfo } nav && nav.Expression == rootParam)
                    {
                        if (!model.TryGetReferenceNavigation(typeof(TEntity), nav.Member, out var navigation))
                        {
                            throw new NotSupportedException(
                                $"Navigation '{typeof(TEntity).FullName}.{nav.Member.Name}' is not configured. Configure it in OnModelCreating, or ensure schema FK conventions can apply.");
                        }

                        var join = new Db2NavigationJoinPlan(
                            Root: model.GetEntityType(typeof(TEntity)),
                            Navigation: navigation,
                            Target: model.GetEntityType(navigation.TargetClrType));

                        accesses.Add(new Db2NavigationMemberAccessPlan(join, member.Member));
                        return;
                    }

                    // direct member access is not a navigation (unless used as x.Nav itself, which we don’t support)
                    Visit(member.Expression);
                    return;
                }
                case MethodCallExpression call:
                    Visit(call.Object);
                    foreach (var a in call.Arguments)
                        Visit(a);
                    return;
                case BinaryExpression bin:
                    Visit(bin.Left);
                    Visit(bin.Right);
                    return;
                case ConditionalExpression cond:
                    Visit(cond.Test);
                    Visit(cond.IfTrue);
                    Visit(cond.IfFalse);
                    return;
                case NewExpression n:
                    foreach (var a in n.Arguments)
                        Visit(a);
                    return;
                case MemberInitExpression mi:
                    Visit(mi.NewExpression);
                    foreach (var bnd in mi.Bindings)
                    {
                        if (bnd is MemberAssignment ma)
                            Visit(ma.Expression);
                    }
                    return;
                default:
                    return;
            }
        }

        Visit(lambda.Body);

        return accesses;
    }

    private static Expression UnwrapConvert(Expression expression)
        => expression is UnaryExpression { NodeType: ExpressionType.Convert or ExpressionType.ConvertChecked } u
            ? u.Operand
            : expression;

    private static bool TryParseNavigationStringPredicate(
        Expression expression,
        ParameterExpression rootParam,
        out MemberInfo navMember,
        out MemberInfo targetMember,
        out Db2NavigationStringMatchKind matchKind,
        out string needle)
    {
        navMember = null!;
        targetMember = null!;
        matchKind = default;
        needle = string.Empty;

        expression = UnwrapConvert(expression);

        if (expression is BinaryExpression { NodeType: ExpressionType.Equal } eq)
        {
            if (!TryGetStringConstant(eq.Left, out var constant) && !TryGetStringConstant(eq.Right, out constant))
                return false;

            var memberSide = ReferenceEquals(eq.Left, constant.Expression) ? eq.Right : eq.Left;
            if (!TryGetNavThenMemberAccess(memberSide, rootParam, out navMember, out targetMember))
                return false;

            needle = constant.Value;
            matchKind = Db2NavigationStringMatchKind.Equals;
            return true;
        }

        if (expression is MethodCallExpression { Method.DeclaringType: { } dt } call && dt == typeof(string))
        {
            if (call.Arguments.Count != 1 || call.Arguments[0] is not ConstantExpression { Value: string s })
                return false;

            if (!TryGetNavThenMemberAccess(call.Object, rootParam, out navMember, out targetMember))
                return false;

            needle = s;
            matchKind = call.Method.Name switch
            {
                nameof(string.Contains) => Db2NavigationStringMatchKind.Contains,
                nameof(string.StartsWith) => Db2NavigationStringMatchKind.StartsWith,
                nameof(string.EndsWith) => Db2NavigationStringMatchKind.EndsWith,
                _ => Db2NavigationStringMatchKind.Equals,
            };

            return call.Method.Name is nameof(string.Contains) or nameof(string.StartsWith) or nameof(string.EndsWith);
        }

        return false;

        static bool TryGetStringConstant(Expression expr, out (string Value, ConstantExpression Expression) constant)
        {
            if (expr is ConstantExpression { Value: string s1 } c)
            {
                constant = (s1, c);
                return true;
            }

            constant = default;
            return false;
        }

        static bool TryGetNavThenMemberAccess(
            Expression? expr,
            ParameterExpression root,
            out MemberInfo nav,
            out MemberInfo related)
        {
            nav = null!;
            related = null!;

            if (expr is null)
                return false;

            expr = expr is UnaryExpression { NodeType: ExpressionType.Convert or ExpressionType.ConvertChecked } u2 ? u2.Operand : expr;

            if (expr is not MemberExpression { Member: PropertyInfo or FieldInfo } relatedExpr)
                return false;

            var navExpr = relatedExpr.Expression;
            navExpr = navExpr is UnaryExpression { NodeType: ExpressionType.Convert or ExpressionType.ConvertChecked } u3 ? u3.Operand : navExpr;

            if (navExpr is not MemberExpression { Member: PropertyInfo or FieldInfo } navMemberExpr)
                return false;

            if (navMemberExpr.Expression != root)
                return false;

            nav = navMemberExpr.Member;
            related = relatedExpr.Member;
            return true;
        }
    }
}
