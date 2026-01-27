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

        if (predicate.Parameters is not { Count: 1 } || predicate.Parameters[0].Type != typeof(TEntity))
            return false;

        var rootParam = predicate.Parameters[0];

        var body = UnwrapConvert(predicate.Body);

        if (!TryParseNavigationStringPredicate(body, rootParam, out var navMember, out var targetMember, out var matchKind, out var needle))
            return false;

        if (!model.TryGetReferenceNavigation(typeof(TEntity), navMember, out var navigation))
            return false;

        var root = model.GetEntityType(typeof(TEntity));
        var target = model.GetEntityType(navigation.TargetClrType);

        var join = new Db2NavigationJoinPlan(root, navigation, target);

        var rootReq = new Db2SourceRequirements(root.Schema, root.ClrType);
        rootReq.RequireMember(join.RootKeyMember, Db2RequiredColumnKind.JoinKey);

        var targetReq = new Db2SourceRequirements(target.Schema, target.ClrType);
        targetReq.RequireMember(join.TargetKeyMember, Db2RequiredColumnKind.JoinKey);
        targetReq.RequireMember(targetMember, Db2RequiredColumnKind.String);

        if (!target.Schema.TryGetFieldCaseInsensitive(targetMember.Name, out var targetStringFieldSchema))
        {
            throw new NotSupportedException(
                $"Field '{targetMember.Name}' not found in schema for table '{target.TableName}'. " +
                $"This field is required for navigation string predicate on '{typeof(TEntity).FullName}.{navMember.Name}'.");
        }

        plan = new Db2NavigationStringPredicatePlan(
            Join: join,
            TargetStringMember: targetMember,
            TargetStringFieldSchema: targetStringFieldSchema,
            MatchKind: matchKind,
            Needle: needle,
            RootRequirements: rootReq,
            TargetRequirements: targetReq);

        return true;
    }

    public static IReadOnlyList<Db2NavigationMemberAccessPlan> GetNavigationAccesses<TEntity>(
        Db2Model model,
        LambdaExpression lambda)
    {
        ArgumentNullException.ThrowIfNull(model);
        ArgumentNullException.ThrowIfNull(lambda);

        if (lambda.Parameters is not { Count: 1 } || lambda.Parameters[0].Type != typeof(TEntity))
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

                            var rootReq = new Db2SourceRequirements(join.Root.Schema, join.Root.ClrType);
                            rootReq.RequireMember(join.RootKeyMember, Db2RequiredColumnKind.JoinKey);

                            var targetReq = new Db2SourceRequirements(join.Target.Schema, join.Target.ClrType);
                            targetReq.RequireMember(join.TargetKeyMember, Db2RequiredColumnKind.JoinKey);

                            var targetMemberType = GetMemberType(member.Member);
                            targetReq.RequireMember(member.Member, targetMemberType == typeof(string) ? Db2RequiredColumnKind.String : Db2RequiredColumnKind.Scalar);

                            accesses.Add(new Db2NavigationMemberAccessPlan(join, member.Member, rootReq, targetReq));
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

    private static Type GetMemberType(MemberInfo member)
        => member switch
        {
            PropertyInfo p => p.PropertyType,
            FieldInfo f => f.FieldType,
            _ => throw new InvalidOperationException($"Unexpected member type: {member.GetType().FullName}"),
        };

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
            if (!TryGetString(eq.Left, rootParam, out var constant) && !TryGetString(eq.Right, rootParam, out constant))
                return false;

            var memberSide = ExpressionEquals(eq.Left, constant.Expression) ? eq.Right : eq.Left;
            if (!TryGetNavThenMemberAccess(memberSide, rootParam, out navMember, out targetMember))
                return false;

            needle = constant.Value;
            matchKind = Db2NavigationStringMatchKind.Equals;
            return true;
        }

        if (expression is MethodCallExpression { Method.DeclaringType: { } dt } call && dt == typeof(string))
        {
            if (call.Arguments is not { Count: 1 } || !TryGetString(call.Arguments[0], rootParam, out var constant))
                return false;

            if (!TryGetNavThenMemberAccess(call.Object, rootParam, out navMember, out targetMember))
                return false;

            needle = constant.Value;
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

        static bool TryGetString(Expression expr, ParameterExpression root, out (string Value, Expression Expression) constant)
        {
            expr = expr is UnaryExpression { NodeType: ExpressionType.Convert or ExpressionType.ConvertChecked } u ? u.Operand : expr;

            if (expr is ConstantExpression { Value: string s1 })
            {
                constant = (s1, expr);
                return true;
            }

            if (ContainsParameter(expr, root))
            {
                constant = default;
                return false;
            }

            try
            {
                var lambda = Expression.Lambda<Func<string?>>(Expression.Convert(expr, typeof(string)));
                var value = lambda.Compile().Invoke();
                if (value is null)
                {
                    constant = default;
                    return false;
                }

                constant = (value, expr);
                return true;
            }
            catch
            {
                constant = default;
                return false;
            }
        }

        static bool ContainsParameter(Expression expr, ParameterExpression parameter)
        {
            var found = false;

            void Visit(Expression? e)
            {
                if (found || e is null)
                    return;

                e = e is UnaryExpression { NodeType: ExpressionType.Convert or ExpressionType.ConvertChecked } u ? u.Operand : e;

                if (ReferenceEquals(e, parameter))
                {
                    found = true;
                    return;
                }

                switch (e)
                {
                    case MemberExpression me:
                        Visit(me.Expression);
                        return;
                    case MethodCallExpression mc:
                        Visit(mc.Object);
                        foreach (var a in mc.Arguments)
                            Visit(a);
                        return;
                    case BinaryExpression b:
                        Visit(b.Left);
                        Visit(b.Right);
                        return;
                    case ConditionalExpression c:
                        Visit(c.Test);
                        Visit(c.IfTrue);
                        Visit(c.IfFalse);
                        return;
                    case NewExpression n:
                        foreach (var a in n.Arguments)
                            Visit(a);
                        return;
                    default:
                        return;
                }
            }

            Visit(expr);
            return found;
        }

        static bool ExpressionEquals(Expression a, Expression b)
        {
            a = a is UnaryExpression { NodeType: ExpressionType.Convert or ExpressionType.ConvertChecked } ua ? ua.Operand : a;
            b = b is UnaryExpression { NodeType: ExpressionType.Convert or ExpressionType.ConvertChecked } ub ? ub.Operand : b;
            return ReferenceEquals(a, b);
        }
    }
}
