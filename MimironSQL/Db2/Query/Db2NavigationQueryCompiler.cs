using System.Linq;
using System.Linq.Expressions;
using System.Reflection;

using MimironSQL.Db2;
using MimironSQL.Db2.Model;
using MimironSQL.Db2.Schema;
using MimironSQL.Db2.Wdc5;

namespace MimironSQL.Db2.Query;

internal static class Db2NavigationQueryCompiler
{
    public static bool TryCompileSemiJoinPredicate<TEntity>(
        Db2Model model,
        Wdc5File rootFile,
        Db2TableSchema rootSchema,
        Func<string, (Wdc5File File, Db2TableSchema Schema)> tableResolver,
        Expression<Func<TEntity, bool>> predicate,
        out Func<Wdc5Row, bool> rowPredicate)
    {
        rowPredicate = _ => true;

        var body = predicate.Body is UnaryExpression { NodeType: ExpressionType.Convert or ExpressionType.ConvertChecked } u
            ? u.Operand
            : predicate.Body;

        if (body is BinaryExpression { NodeType: ExpressionType.AndAlso } andAlso)
        {
            var p = predicate.Parameters[0];
            var left = Expression.Lambda<Func<TEntity, bool>>(andAlso.Left, p);
            var right = Expression.Lambda<Func<TEntity, bool>>(andAlso.Right, p);

            if (Db2NavigationQueryTranslator.TryTranslateStringPredicate(model, left, out var leftPlan) &&
                Db2NavigationQueryTranslator.TryTranslateStringPredicate(model, right, out var rightPlan) &&
                leftPlan.Join.Navigation.NavigationMember == rightPlan.Join.Navigation.NavigationMember)
            {
                var leftIds = FindMatchingIds(tableResolver, leftPlan);
                var rightIds = FindMatchingIds(tableResolver, rightPlan);
                leftIds.IntersectWith(rightIds);

                rowPredicate = leftPlan.Join.Kind switch
                {
                    Db2ReferenceNavigationKind.ForeignKeyToPrimaryKey => CompileForeignKeySemiJoin(rootSchema, leftPlan.Join.Navigation.SourceKeyMember, leftIds),
                    Db2ReferenceNavigationKind.SharedPrimaryKeyOneToOne => row => row.Id != 0 && leftIds.Contains(row.Id),
                    _ => _ => false,
                };

                return true;
            }

            if (Db2NavigationQueryTranslator.TryTranslateStringPredicate(model, left, out var navPlanLeft) &&
                Db2RowPredicateCompiler.TryCompile(rootFile, rootSchema, right, out var rightRowPredicate))
            {
                var ids = FindMatchingIds(tableResolver, navPlanLeft);
                var nav = navPlanLeft.Join.Kind switch
                {
                    Db2ReferenceNavigationKind.ForeignKeyToPrimaryKey => CompileForeignKeySemiJoin(rootSchema, navPlanLeft.Join.Navigation.SourceKeyMember, ids),
                    Db2ReferenceNavigationKind.SharedPrimaryKeyOneToOne => row => row.Id != 0 && ids.Contains(row.Id),
                    _ => _ => false,
                };

                rowPredicate = row => nav(row) && rightRowPredicate(row);
                return true;
            }

            if (Db2NavigationQueryTranslator.TryTranslateStringPredicate(model, right, out var navPlanRight) &&
                Db2RowPredicateCompiler.TryCompile(rootFile, rootSchema, left, out var leftRowPredicate))
            {
                var ids = FindMatchingIds(tableResolver, navPlanRight);
                var nav = navPlanRight.Join.Kind switch
                {
                    Db2ReferenceNavigationKind.ForeignKeyToPrimaryKey => CompileForeignKeySemiJoin(rootSchema, navPlanRight.Join.Navigation.SourceKeyMember, ids),
                    Db2ReferenceNavigationKind.SharedPrimaryKeyOneToOne => row => row.Id != 0 && ids.Contains(row.Id),
                    _ => _ => false,
                };

                rowPredicate = row => leftRowPredicate(row) && nav(row);
                return true;
            }
        }

        if (!Db2NavigationQueryTranslator.TryTranslateStringPredicate(model, predicate, out var plan))
            return false;

        var matchingIds = FindMatchingIds(tableResolver, plan);

        rowPredicate = plan.Join.Kind switch
        {
            Db2ReferenceNavigationKind.ForeignKeyToPrimaryKey => CompileForeignKeySemiJoin(rootSchema, plan.Join.Navigation.SourceKeyMember, matchingIds),
            Db2ReferenceNavigationKind.SharedPrimaryKeyOneToOne => row => row.Id != 0 && matchingIds.Contains(row.Id),
            _ => _ => false,
        };

        return true;
    }

    private static HashSet<int> FindMatchingIds(
        Func<string, (Wdc5File File, Db2TableSchema Schema)> tableResolver,
        Db2NavigationStringPredicatePlan plan)
    {
        var (relatedFile, relatedSchema) = tableResolver(plan.Join.Target.TableName);

        if (!relatedSchema.TryGetField(plan.TargetStringMember.Name, out var relatedField))
            return [];

        var relatedFieldIndex = relatedField.ColumnStartIndex;

        HashSet<int> matchingIds = [];

        if (plan.MatchKind is Db2NavigationStringMatchKind.Contains or Db2NavigationStringMatchKind.StartsWith or Db2NavigationStringMatchKind.EndsWith)
        {
            var mk = plan.MatchKind switch
            {
                Db2NavigationStringMatchKind.Contains => Db2StringMatchKind.Contains,
                Db2NavigationStringMatchKind.StartsWith => Db2StringMatchKind.StartsWith,
                Db2NavigationStringMatchKind.EndsWith => Db2StringMatchKind.EndsWith,
                _ => Db2StringMatchKind.Contains,
            };

            var isDenseOptimizable = !relatedFile.Header.Flags.HasFlag(Db2Flags.Sparse) && !relatedField.IsVirtual;
            if (isDenseOptimizable)
            {
                var starts = Db2DenseStringScanner.FindStartOffsets(relatedFile.DenseStringTableBytes, plan.Needle, mk);
                var accessor = new Db2FieldAccessor(relatedField);

                foreach (var relatedRow in relatedFile.EnumerateRows())
                {
                    var ok = mk switch
                    {
                        Db2StringMatchKind.Contains => Db2DenseStringMatch.Contains(relatedRow, accessor, starts),
                        Db2StringMatchKind.StartsWith => Db2DenseStringMatch.StartsWith(relatedRow, accessor, starts),
                        Db2StringMatchKind.EndsWith => Db2DenseStringMatch.EndsWith(relatedRow, accessor, starts),
                        _ => false,
                    };

                    if (ok)
                        matchingIds.Add(relatedRow.Id);
                }

                return matchingIds;
            }

            foreach (var relatedRow in relatedFile.EnumerateRows())
            {
                if (!relatedRow.TryGetString(relatedFieldIndex, out var s))
                    continue;

                var ok = plan.MatchKind switch
                {
                    Db2NavigationStringMatchKind.Contains => s.Contains(plan.Needle, StringComparison.Ordinal),
                    Db2NavigationStringMatchKind.StartsWith => s.StartsWith(plan.Needle, StringComparison.Ordinal),
                    Db2NavigationStringMatchKind.EndsWith => s.EndsWith(plan.Needle, StringComparison.Ordinal),
                    _ => false,
                };

                if (ok)
                    matchingIds.Add(relatedRow.Id);
            }

            return matchingIds;
        }

        foreach (var relatedRow in relatedFile.EnumerateRows())
        {
            if (relatedRow.TryGetString(relatedFieldIndex, out var s) && s == plan.Needle)
                matchingIds.Add(relatedRow.Id);
        }

        return matchingIds;
    }

    private static Func<Wdc5Row, bool> CompileForeignKeySemiJoin(Db2TableSchema rootSchema, MemberInfo foreignKeyMember, HashSet<int> ids)
    {
        var fkField = rootSchema.Fields.FirstOrDefault(f => f.Name.Equals(foreignKeyMember.Name, StringComparison.OrdinalIgnoreCase));
        if (fkField.Name is null)
            return _ => false;

        var rootFkIndex = fkField.ColumnStartIndex;
        return row =>
        {
            var fk = Convert.ToInt32(row.GetScalar<long>(rootFkIndex));
            return fk != 0 && ids.Contains(fk);
        };
    }
}
