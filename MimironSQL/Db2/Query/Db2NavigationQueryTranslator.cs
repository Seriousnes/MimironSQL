using System.Linq.Expressions;
using System.Reflection;

using MimironSQL.Db2.Model;
using MimironSQL.Db2.Schema;
using MimironSQL.Extensions;

namespace MimironSQL.Db2.Query;

internal static class Db2NavigationQueryTranslator
{
    public static bool TryTranslateCollectionAnyPredicate<TEntity>(
        Db2Model model,
        Expression<Func<TEntity, bool>> predicate,
        out (Db2CollectionNavigation Navigation, LambdaExpression? DependentPredicate) plan)
    {
        ArgumentNullException.ThrowIfNull(model);
        ArgumentNullException.ThrowIfNull(predicate);

        plan = default;

        if (predicate.Parameters is not { Count: 1 } || predicate.Parameters[0].Type != typeof(TEntity))
            return false;

        var rootParam = predicate.Parameters[0];
        var body = predicate.Body.UnwrapConvert()!;

        if (body is not MethodCallExpression call)
            return false;

        if (!IsEnumerableAny(call.Method))
            return false;

        if (call.Arguments.Count is < 1 or > 2)
            return false;

        var source = call.Arguments[0].UnwrapConvert()!;
        if (source is not MemberExpression { Member: PropertyInfo or FieldInfo } member)
            return false;

        if (member.Expression != rootParam)
            return false;

        var navMember = member.Member;
        if (!model.TryGetCollectionNavigation(typeof(TEntity), navMember, out var nav))
            return false;

        LambdaExpression? dependentPredicate = null;
        if (call.Arguments.Count == 2)
        {
            var arg = call.Arguments[1];
            if (arg is UnaryExpression { NodeType: ExpressionType.Quote } q)
                arg = q.Operand;

            if (arg is not LambdaExpression lambda)
                return false;

            dependentPredicate = lambda;
        }

        plan = (nav, dependentPredicate);
        return true;

        static bool IsEnumerableAny(MethodInfo method)
            => method is { Name: nameof(Enumerable.Any), IsStatic: true } && method.DeclaringType == typeof(Enumerable);
    }

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

        var body = predicate.Body.UnwrapConvert()!;

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
        var body = predicate.Body.UnwrapConvert()!;

        if (!TryParseNavigationScalarPredicate(body, rootParam, out var navMember, out var targetMember, out var comparisonKind, out var comparisonValueExpression, out var scalarType))
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

        plan = CreateScalarPredicatePlan(
            join,
            targetMember,
            targetFieldSchema,
            comparisonKind,
            comparisonValueExpression,
            scalarType,
            rootReq,
            targetReq);

        return true;
    }

    private static Db2NavigationScalarPredicatePlan CreateScalarPredicatePlan(
        Db2NavigationJoinPlan join,
        MemberInfo targetMember,
        Db2FieldSchema targetFieldSchema,
        Db2ScalarComparisonKind comparisonKind,
        Expression comparisonValueExpression,
        Type scalarType,
        Db2SourceRequirements rootReq,
        Db2SourceRequirements targetReq)
    {
        var underlying = scalarType.UnwrapNullable();
        if (underlying.IsEnum)
            underlying = Enum.GetUnderlyingType(underlying);

        return Type.GetTypeCode(underlying) switch
        {
            TypeCode.Boolean => new Db2NavigationScalarPredicatePlan<bool>(
                Join: join,
                TargetScalarMember: targetMember,
                TargetScalarFieldSchema: targetFieldSchema,
                ComparisonKind: comparisonKind,
                ComparisonValue: EvaluateScalarConstant<bool>(comparisonValueExpression),
                RootRequirements: rootReq,
                TargetRequirements: targetReq),

            TypeCode.Byte => new Db2NavigationScalarPredicatePlan<byte>(
                Join: join,
                TargetScalarMember: targetMember,
                TargetScalarFieldSchema: targetFieldSchema,
                ComparisonKind: comparisonKind,
                ComparisonValue: EvaluateScalarConstant<byte>(comparisonValueExpression),
                RootRequirements: rootReq,
                TargetRequirements: targetReq),

            TypeCode.SByte => new Db2NavigationScalarPredicatePlan<sbyte>(
                Join: join,
                TargetScalarMember: targetMember,
                TargetScalarFieldSchema: targetFieldSchema,
                ComparisonKind: comparisonKind,
                ComparisonValue: EvaluateScalarConstant<sbyte>(comparisonValueExpression),
                RootRequirements: rootReq,
                TargetRequirements: targetReq),

            TypeCode.Int16 => new Db2NavigationScalarPredicatePlan<short>(
                Join: join,
                TargetScalarMember: targetMember,
                TargetScalarFieldSchema: targetFieldSchema,
                ComparisonKind: comparisonKind,
                ComparisonValue: EvaluateScalarConstant<short>(comparisonValueExpression),
                RootRequirements: rootReq,
                TargetRequirements: targetReq),

            TypeCode.UInt16 => new Db2NavigationScalarPredicatePlan<ushort>(
                Join: join,
                TargetScalarMember: targetMember,
                TargetScalarFieldSchema: targetFieldSchema,
                ComparisonKind: comparisonKind,
                ComparisonValue: EvaluateScalarConstant<ushort>(comparisonValueExpression),
                RootRequirements: rootReq,
                TargetRequirements: targetReq),

            TypeCode.Int32 => new Db2NavigationScalarPredicatePlan<int>(
                Join: join,
                TargetScalarMember: targetMember,
                TargetScalarFieldSchema: targetFieldSchema,
                ComparisonKind: comparisonKind,
                ComparisonValue: EvaluateScalarConstant<int>(comparisonValueExpression),
                RootRequirements: rootReq,
                TargetRequirements: targetReq),

            TypeCode.UInt32 => new Db2NavigationScalarPredicatePlan<uint>(
                Join: join,
                TargetScalarMember: targetMember,
                TargetScalarFieldSchema: targetFieldSchema,
                ComparisonKind: comparisonKind,
                ComparisonValue: EvaluateScalarConstant<uint>(comparisonValueExpression),
                RootRequirements: rootReq,
                TargetRequirements: targetReq),

            TypeCode.Int64 => new Db2NavigationScalarPredicatePlan<long>(
                Join: join,
                TargetScalarMember: targetMember,
                TargetScalarFieldSchema: targetFieldSchema,
                ComparisonKind: comparisonKind,
                ComparisonValue: EvaluateScalarConstant<long>(comparisonValueExpression),
                RootRequirements: rootReq,
                TargetRequirements: targetReq),

            TypeCode.UInt64 => new Db2NavigationScalarPredicatePlan<ulong>(
                Join: join,
                TargetScalarMember: targetMember,
                TargetScalarFieldSchema: targetFieldSchema,
                ComparisonKind: comparisonKind,
                ComparisonValue: EvaluateScalarConstant<ulong>(comparisonValueExpression),
                RootRequirements: rootReq,
                TargetRequirements: targetReq),

            TypeCode.Single => new Db2NavigationScalarPredicatePlan<float>(
                Join: join,
                TargetScalarMember: targetMember,
                TargetScalarFieldSchema: targetFieldSchema,
                ComparisonKind: comparisonKind,
                ComparisonValue: EvaluateScalarConstant<float>(comparisonValueExpression),
                RootRequirements: rootReq,
                TargetRequirements: targetReq),

            TypeCode.Double => new Db2NavigationScalarPredicatePlan<double>(
                Join: join,
                TargetScalarMember: targetMember,
                TargetScalarFieldSchema: targetFieldSchema,
                ComparisonKind: comparisonKind,
                ComparisonValue: EvaluateScalarConstant<double>(comparisonValueExpression),
                RootRequirements: rootReq,
                TargetRequirements: targetReq),

            _ => throw new NotSupportedException($"Scalar predicate type '{scalarType.FullName}' is not supported."),
        };
    }

    private static T EvaluateScalarConstant<T>(Expression expr) where T : unmanaged
    {
        expr = expr.UnwrapConvert()!;

        if (expr is ConstantExpression { Value: not null } constant)
        {
            // Allowed: unboxing for expression constants during translation.
            return (T)constant.Value;
        }

        var lambda = Expression.Lambda<Func<T>>(Expression.Convert(expr, typeof(T)));
        return lambda.Compile().Invoke();
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
        var body = predicate.Body.UnwrapConvert()!;

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

            expr = expr.UnwrapConvert();

            switch (expr)
            {
                case MemberExpression { Member: PropertyInfo or FieldInfo } member:
                    {
                        // 1-hop: x.Nav.Member
                        if (member.Expression.UnwrapConvert() is MemberExpression { Member: PropertyInfo or FieldInfo } nav && nav.Expression == rootParam)
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

                            var targetMemberType = member.Member.GetMemberType();
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

        expression = expression.UnwrapConvert()!;

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

            expr = expr.UnwrapConvert();

            if (expr is not MemberExpression { Member: PropertyInfo or FieldInfo } relatedExpr)
                return false;

            var navExpr = relatedExpr.Expression;
            navExpr = navExpr.UnwrapConvert();

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
            expr = expr.UnwrapConvert()!;

            if (expr is ConstantExpression { Value: string s1 })
            {
                constant = (s1, expr);
                return true;
            }

            if (expr.ContainsParameter(root))
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

        static bool ExpressionEquals(Expression a, Expression b)
        {
            a = a.UnwrapConvert()!;
            b = b.UnwrapConvert()!;
            return ReferenceEquals(a, b);
        }
    }

    private static bool TryParseNavigationScalarPredicate(
        Expression expression,
        ParameterExpression rootParam,
        out MemberInfo navMember,
        out MemberInfo targetMember,
        out Db2ScalarComparisonKind comparisonKind,
        out Expression comparisonValueExpression,
        out Type scalarType)
    {
        navMember = null!;
        targetMember = null!;
        comparisonKind = default;
        comparisonValueExpression = null!;
        scalarType = null!;

        expression = expression.UnwrapConvert()!;

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

        if (leftIsNav && TryGetScalarValueExpression(bin.Right, rootParam, out comparisonValueExpression, out scalarType))
        {
            navMember = leftNav;
            targetMember = leftMember;
            return true;
        }

        if (rightIsNav && TryGetScalarValueExpression(bin.Left, rootParam, out comparisonValueExpression, out scalarType))
        {
            navMember = rightNav;
            targetMember = rightMember;
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

            expr = expr.UnwrapConvert();

            if (expr is not MemberExpression { Member: PropertyInfo or FieldInfo } relatedExpr)
                return false;

            var navExpr = relatedExpr.Expression;
            navExpr = navExpr.UnwrapConvert();

            if (navExpr is not MemberExpression { Member: PropertyInfo or FieldInfo } navMemberExpr)
                return false;

            if (navMemberExpr.Expression != root)
                return false;

            nav = navMemberExpr.Member;
            related = relatedExpr.Member;
            return true;
        }

        static bool TryGetScalarValueExpression(Expression expr, ParameterExpression root, out Expression valueExpression, out Type type)
        {
            expr = expr.UnwrapConvert()!;
            type = expr.Type;

            if (expr.ContainsParameter(root))
            {
                valueExpression = null!;
                return false;
            }

            if (expr is ConstantExpression { Value: not null } constant)
            {
                valueExpression = constant;
                return type.IsScalarType();
            }

            // Allow captured values and other parameter-free expressions by evaluating later as a typed delegate.
            valueExpression = expr;
            return type.IsScalarType();
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

    }

    private static bool TryParseNavigationNullCheck(
        Expression expression,
        ParameterExpression rootParam,
        out MemberInfo navMember,
        out bool isNotNull)
    {
        navMember = null!;
        isNotNull = false;

        expression = expression.UnwrapConvert()!;

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
            expr = expr.UnwrapConvert()!;
            return expr is ConstantExpression { Value: null };
        }

        static bool TryGetNavigation(Expression? expr, ParameterExpression root, out MemberInfo nav)
        {
            nav = null!;

            if (expr is null)
                return false;

            expr = expr.UnwrapConvert();

            if (expr is not MemberExpression { Member: PropertyInfo or FieldInfo } navExpr)
                return false;

            if (navExpr.Expression != root)
                return false;

            nav = navExpr.Member;
            return true;
        }
    }
}
