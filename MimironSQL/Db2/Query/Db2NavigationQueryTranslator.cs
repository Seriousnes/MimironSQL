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

    public static bool TryTranslateScalarPredicate<TEntity>(
        Db2Model model,
        Expression<Func<TEntity, bool>> predicate,
        out Db2NavigationScalarPredicatePlan plan)
    {
        ArgumentNullException.ThrowIfNull(model);
        ArgumentNullException.ThrowIfNull(predicate);

        plan = null!;

        if (predicate.Parameters is not { Count: 1 } || predicate.Parameters[0].Type != typeof(TEntity))
            return false;

        var rootParam = predicate.Parameters[0];
        var body = UnwrapConvert(predicate.Body);

        if (!TryParseNavigationScalarPredicate(body, rootParam, out var navMember, out var targetMember, out var comparisonKind, out var comparisonValue, out var scalarType))
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
        targetReq.RequireMember(targetMember, Db2RequiredColumnKind.Scalar);

        if (!target.Schema.TryGetFieldCaseInsensitive(targetMember.Name, out var targetFieldSchema))
        {
            throw new NotSupportedException(
                $"Field '{targetMember.Name}' not found in schema for table '{target.TableName}'. " +
                $"This field is required for navigation scalar predicate on '{typeof(TEntity).FullName}.{navMember.Name}'.");
        }

        plan = new Db2NavigationScalarPredicatePlan(
            Join: join,
            TargetScalarMember: targetMember,
            TargetScalarFieldSchema: targetFieldSchema,
            ComparisonKind: comparisonKind,
            ComparisonValue: comparisonValue,
            ScalarType: scalarType,
            RootRequirements: rootReq,
            TargetRequirements: targetReq);

        return true;
    }

    public static bool TryTranslateNullCheck<TEntity>(
        Db2Model model,
        Expression<Func<TEntity, bool>> predicate,
        out Db2NavigationNullCheckPlan plan)
    {
        ArgumentNullException.ThrowIfNull(model);
        ArgumentNullException.ThrowIfNull(predicate);

        plan = null!;

        if (predicate.Parameters is not { Count: 1 } || predicate.Parameters[0].Type != typeof(TEntity))
            return false;

        var rootParam = predicate.Parameters[0];
        var body = UnwrapConvert(predicate.Body);

        if (!TryParseNavigationNullCheck(body, rootParam, out var navMember, out var isNotNull))
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

        plan = new Db2NavigationNullCheckPlan(
            Join: join,
            IsNotNull: isNotNull,
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

    private static bool TryParseNavigationScalarPredicate(
        Expression expression,
        ParameterExpression rootParam,
        out MemberInfo navMember,
        out MemberInfo targetMember,
        out Db2ScalarComparisonKind comparisonKind,
        out object comparisonValue,
        out Type scalarType)
    {
        navMember = null!;
        targetMember = null!;
        comparisonKind = default;
        comparisonValue = null!;
        scalarType = null!;

        expression = UnwrapConvert(expression);

        if (expression is not BinaryExpression bin)
            return false;

        comparisonKind = bin.NodeType switch
        {
            ExpressionType.Equal => Db2ScalarComparisonKind.Equal,
            ExpressionType.NotEqual => Db2ScalarComparisonKind.NotEqual,
            ExpressionType.LessThan => Db2ScalarComparisonKind.LessThan,
            ExpressionType.LessThanOrEqual => Db2ScalarComparisonKind.LessThanOrEqual,
            ExpressionType.GreaterThan => Db2ScalarComparisonKind.GreaterThan,
            ExpressionType.GreaterThanOrEqual => Db2ScalarComparisonKind.GreaterThanOrEqual,
            _ => (Db2ScalarComparisonKind)(-1), // Sentinel value for unsupported operators
        };

        // Check if the operator is supported
        if ((int)comparisonKind == -1)
            return false;

        var leftIsNav = TryGetNavThenMemberAccess(bin.Left, rootParam, out var leftNav, out var leftMember);
        var rightIsNav = TryGetNavThenMemberAccess(bin.Right, rootParam, out var rightNav, out var rightMember);

        if (leftIsNav && TryGetScalarValue(bin.Right, rootParam, out var rightValue, out scalarType))
        {
            navMember = leftNav;
            targetMember = leftMember;
            comparisonValue = rightValue;
            return true;
        }

        if (rightIsNav && TryGetScalarValue(bin.Left, rootParam, out var leftValue, out scalarType))
        {
            navMember = rightNav;
            targetMember = rightMember;
            comparisonValue = leftValue;
            comparisonKind = FlipComparison(comparisonKind);
            return true;
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

            expr = UnwrapConvert(expr);

            if (expr is not MemberExpression { Member: PropertyInfo or FieldInfo } relatedExpr)
                return false;

            var navExpr = relatedExpr.Expression;
            navExpr = UnwrapConvert(navExpr);

            if (navExpr is not MemberExpression { Member: PropertyInfo or FieldInfo } navMemberExpr)
                return false;

            if (navMemberExpr.Expression != root)
                return false;

            nav = navMemberExpr.Member;
            related = relatedExpr.Member;
            return true;
        }

        static bool TryGetScalarValue(Expression expr, ParameterExpression root, out object value, out Type type)
        {
            expr = UnwrapConvert(expr);
            type = expr.Type;

            if (expr is ConstantExpression { Value: not null } constant)
            {
                value = constant.Value;
                return IsScalarType(type);
            }

            if (ContainsParameter(expr, root))
            {
                value = null!;
                return false;
            }

            try
            {
                var lambda = Expression.Lambda<Func<object>>(Expression.Convert(expr, typeof(object)));
                value = lambda.Compile().Invoke();
                return value is not null && IsScalarType(type);
            }
            catch
            {
                value = null!;
                return false;
            }
        }

        static bool IsScalarType(Type type)
        {
            return type.IsPrimitive || type == typeof(decimal) || type.IsEnum;
        }

        static Db2ScalarComparisonKind FlipComparison(Db2ScalarComparisonKind kind)
        {
            return kind switch
            {
                Db2ScalarComparisonKind.LessThan => Db2ScalarComparisonKind.GreaterThan,
                Db2ScalarComparisonKind.LessThanOrEqual => Db2ScalarComparisonKind.GreaterThanOrEqual,
                Db2ScalarComparisonKind.GreaterThan => Db2ScalarComparisonKind.LessThan,
                Db2ScalarComparisonKind.GreaterThanOrEqual => Db2ScalarComparisonKind.LessThanOrEqual,
                _ => kind,
            };
        }

        static bool ContainsParameter(Expression expr, ParameterExpression parameter)
        {
            var found = false;

            void Visit(Expression? e)
            {
                if (found || e is null)
                    return;

                e = UnwrapConvert(e);

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
    }

    private static bool TryParseNavigationNullCheck(
        Expression expression,
        ParameterExpression rootParam,
        out MemberInfo navMember,
        out bool isNotNull)
    {
        navMember = null!;
        isNotNull = false;

        expression = UnwrapConvert(expression);

        if (expression is not BinaryExpression bin)
            return false;

        if (bin.NodeType is not (ExpressionType.Equal or ExpressionType.NotEqual))
            return false;

        isNotNull = bin.NodeType == ExpressionType.NotEqual;

        var leftIsNull = IsNullConstant(bin.Left);
        var rightIsNull = IsNullConstant(bin.Right);

        if (leftIsNull && TryGetNavigation(bin.Right, rootParam, out navMember))
            return true;

        if (rightIsNull && TryGetNavigation(bin.Left, rootParam, out navMember))
            return true;

        return false;

        static bool IsNullConstant(Expression expr)
        {
            expr = UnwrapConvert(expr);
            return expr is ConstantExpression { Value: null };
        }

        static bool TryGetNavigation(Expression? expr, ParameterExpression root, out MemberInfo nav)
        {
            nav = null!;

            if (expr is null)
                return false;

            expr = UnwrapConvert(expr);

            if (expr is not MemberExpression { Member: PropertyInfo or FieldInfo } navExpr)
                return false;

            if (navExpr.Expression != root)
                return false;

            nav = navExpr.Member;
            return true;
        }
    }
}
