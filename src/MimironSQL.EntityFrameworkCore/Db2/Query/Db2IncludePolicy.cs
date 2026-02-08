using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;

using MimironSQL.Db2.Model;

namespace MimironSQL.Db2.Query;

internal static class Db2IncludePolicy
{
    public static bool UsesRootNavigation(Db2Model model, LambdaExpression lambda)
        => GetRootNavigationMembers(model, lambda).Count != 0;

    public static void ThrowIfNavigationRequiresInclude(Db2Model model, HashSet<MemberInfo> includedRootMembers, Db2QueryOperation op)
    {
        ArgumentNullException.ThrowIfNull(model);
        ArgumentNullException.ThrowIfNull(includedRootMembers);
        ArgumentNullException.ThrowIfNull(op);

        var lambda = op switch
        {
            Db2WhereOperation w => w.Predicate,
            Db2SelectOperation s => s.Selector,
            _ => null,
        };

        if (lambda is null)
            return;

        ThrowIfNavigationRequiresInclude(model, includedRootMembers, lambda);
    }

    public static void ThrowIfNavigationRequiresInclude(Db2Model model, HashSet<MemberInfo> includedRootMembers, LambdaExpression lambda)
    {
        ArgumentNullException.ThrowIfNull(model);
        ArgumentNullException.ThrowIfNull(includedRootMembers);
        ArgumentNullException.ThrowIfNull(lambda);

        var used = GetRootNavigationMembers(model, lambda);
        if (used.Count == 0)
            return;

        for (var i = 0; i < used.Count; i++)
        {
            var member = used[i];
            if (includedRootMembers.Contains(member))
                continue;

            var entityType = lambda.Parameters is { Count: 1 } ? lambda.Parameters[0].Type : typeof(object);
            throw new NotSupportedException(
                $"Navigation '{entityType.FullName}.{member.Name}' is used in the query and requires an explicit Include(...) for this provider.");
        }
    }

    private static IReadOnlyList<MemberInfo> GetRootNavigationMembers(Db2Model model, LambdaExpression lambda)
    {
        ArgumentNullException.ThrowIfNull(model);
        ArgumentNullException.ThrowIfNull(lambda);

        if (lambda.Parameters is not { Count: 1 })
            return [];

        var rootParam = lambda.Parameters[0];
        var entityType = rootParam.Type;

        var members = new List<MemberInfo>();

        void Visit(Expression? expr)
        {
            if (expr is null)
                return;

            expr = UnwrapConvert(expr);

            switch (expr)
            {
                case MemberExpression member:
                    {
                        var unwrapped = member.Expression is null ? null : UnwrapConvert(member.Expression);

                        if (unwrapped == rootParam && member.Member is PropertyInfo or FieldInfo)
                        {
                            if (model.TryGetReferenceNavigation(entityType, member.Member, out _)
                                || model.TryGetCollectionNavigation(entityType, member.Member, out _))
                            {
                                members.Add(member.Member);
                            }
                        }

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
                case UnaryExpression u:
                    Visit(u.Operand);
                    return;
                default:
                    return;
            }
        }

        Visit(lambda.Body);

        if (members.Count <= 1)
            return members;

        return [.. members.Distinct()];
    }

    private static Expression UnwrapConvert(Expression expression)
    {
        while (expression is UnaryExpression { NodeType: ExpressionType.Convert or ExpressionType.ConvertChecked } u)
            expression = u.Operand;

        return expression;
    }
}
