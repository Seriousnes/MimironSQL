using System.Linq;
using System.Linq.Expressions;
using System.Reflection;

using MimironSQL.Db2;
using MimironSQL.Db2.Model;
using MimironSQL.Db2.Schema;
using MimironSQL.Db2.Wdc5;
using MimironSQL.Extensions;

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

            // Null check + scalar predicate on same navigation
            if (Db2NavigationQueryTranslator.TryTranslateNullCheck(model, left, out var nullCheckLeft) && nullCheckLeft.IsNotNull &&
                Db2NavigationQueryTranslator.TryTranslateScalarPredicate(model, right, out var scalarRight) &&
                nullCheckLeft.Join.Navigation.NavigationMember == scalarRight.Join.Navigation.NavigationMember)
            {
                var existingIds = FindExistingTargetIds(tableResolver, nullCheckLeft);
                var scalarIds = FindMatchingIdsScalar(tableResolver, scalarRight);
                existingIds.IntersectWith(scalarIds);

                rowPredicate = nullCheckLeft.Join.Kind switch
                {
                    Db2ReferenceNavigationKind.ForeignKeyToPrimaryKey => CompileForeignKeySemiJoin(nullCheckLeft.Join.RootKeyFieldSchema, existingIds),
                    Db2ReferenceNavigationKind.SharedPrimaryKeyOneToOne => row => row is { Id: not 0 } && existingIds.Contains(row.Id),
                    _ => _ => false,
                };

                return true;
            }

            // Scalar predicate + null check on same navigation
            if (Db2NavigationQueryTranslator.TryTranslateScalarPredicate(model, left, out var scalarLeft) &&
                Db2NavigationQueryTranslator.TryTranslateNullCheck(model, right, out var nullCheckRight) && nullCheckRight.IsNotNull &&
                scalarLeft.Join.Navigation.NavigationMember == nullCheckRight.Join.Navigation.NavigationMember)
            {
                var scalarIds = FindMatchingIdsScalar(tableResolver, scalarLeft);
                var existingIds = FindExistingTargetIds(tableResolver, nullCheckRight);
                scalarIds.IntersectWith(existingIds);

                rowPredicate = scalarLeft.Join.Kind switch
                {
                    Db2ReferenceNavigationKind.ForeignKeyToPrimaryKey => CompileForeignKeySemiJoin(scalarLeft.Join.RootKeyFieldSchema, scalarIds),
                    Db2ReferenceNavigationKind.SharedPrimaryKeyOneToOne => row => row is { Id: not 0 } && scalarIds.Contains(row.Id),
                    _ => _ => false,
                };

                return true;
            }

            // String + scalar predicates on same navigation (intersection)
            if (Db2NavigationQueryTranslator.TryTranslateStringPredicate(model, left, out var leftStringScalar) &&
                Db2NavigationQueryTranslator.TryTranslateScalarPredicate(model, right, out var rightScalarString) &&
                leftStringScalar.Join.Navigation.NavigationMember == rightScalarString.Join.Navigation.NavigationMember)
            {
                var leftIds = FindMatchingIds(tableResolver, leftStringScalar);
                var rightIds = FindMatchingIdsScalar(tableResolver, rightScalarString);
                leftIds.IntersectWith(rightIds);

                rowPredicate = leftStringScalar.Join.Kind switch
                {
                    Db2ReferenceNavigationKind.ForeignKeyToPrimaryKey => CompileForeignKeySemiJoin(leftStringScalar.Join.RootKeyFieldSchema, leftIds),
                    Db2ReferenceNavigationKind.SharedPrimaryKeyOneToOne => row => row is { Id: not 0 } && leftIds.Contains(row.Id),
                    _ => _ => false,
                };

                return true;
            }

            // Scalar + string predicates on same navigation (intersection)
            if (Db2NavigationQueryTranslator.TryTranslateScalarPredicate(model, left, out var leftScalarString2) &&
                Db2NavigationQueryTranslator.TryTranslateStringPredicate(model, right, out var rightStringScalar2) &&
                leftScalarString2.Join.Navigation.NavigationMember == rightStringScalar2.Join.Navigation.NavigationMember)
            {
                var leftIds = FindMatchingIdsScalar(tableResolver, leftScalarString2);
                var rightIds = FindMatchingIds(tableResolver, rightStringScalar2);
                leftIds.IntersectWith(rightIds);

                rowPredicate = leftScalarString2.Join.Kind switch
                {
                    Db2ReferenceNavigationKind.ForeignKeyToPrimaryKey => CompileForeignKeySemiJoin(leftScalarString2.Join.RootKeyFieldSchema, leftIds),
                    Db2ReferenceNavigationKind.SharedPrimaryKeyOneToOne => row => row is { Id: not 0 } && leftIds.Contains(row.Id),
                    _ => _ => false,
                };

                return true;
            }

            // Two scalar predicates on same navigation
            if (Db2NavigationQueryTranslator.TryTranslateScalarPredicate(model, left, out var leftScalarPlan) &&
                Db2NavigationQueryTranslator.TryTranslateScalarPredicate(model, right, out var rightScalarPlan) &&
                leftScalarPlan.Join.Navigation.NavigationMember == rightScalarPlan.Join.Navigation.NavigationMember)
            {
                var leftIds = FindMatchingIdsScalar(tableResolver, leftScalarPlan);
                var rightIds = FindMatchingIdsScalar(tableResolver, rightScalarPlan);
                leftIds.IntersectWith(rightIds);

                rowPredicate = leftScalarPlan.Join.Kind switch
                {
                    Db2ReferenceNavigationKind.ForeignKeyToPrimaryKey => CompileForeignKeySemiJoin(leftScalarPlan.Join.RootKeyFieldSchema, leftIds),
                    Db2ReferenceNavigationKind.SharedPrimaryKeyOneToOne => row => row is { Id: not 0 } && leftIds.Contains(row.Id),
                    _ => _ => false,
                };

                return true;
            }

            // Cross-navigation: scalar + scalar on different navigations
            if (Db2NavigationQueryTranslator.TryTranslateScalarPredicate(model, left, out var leftScalarCross) &&
                Db2NavigationQueryTranslator.TryTranslateScalarPredicate(model, right, out var rightScalarCross) &&
                leftScalarCross.Join.Navigation.NavigationMember != rightScalarCross.Join.Navigation.NavigationMember)
            {
                var leftIds = FindMatchingIdsScalar(tableResolver, leftScalarCross);
                var rightIds = FindMatchingIdsScalar(tableResolver, rightScalarCross);

                var leftNav = CompileNavigationSemiJoin(leftScalarCross.Join, leftIds);
                var rightNav = CompileNavigationSemiJoin(rightScalarCross.Join, rightIds);

                rowPredicate = row => leftNav(row) && rightNav(row);
                return true;
            }

            // Cross-navigation: string + scalar on different navigations  
            if (Db2NavigationQueryTranslator.TryTranslateStringPredicate(model, left, out var leftStringCross) &&
                Db2NavigationQueryTranslator.TryTranslateScalarPredicate(model, right, out var rightScalarCross2) &&
                leftStringCross.Join.Navigation.NavigationMember != rightScalarCross2.Join.Navigation.NavigationMember)
            {
                var leftIds = FindMatchingIds(tableResolver, leftStringCross);
                var rightIds = FindMatchingIdsScalar(tableResolver, rightScalarCross2);

                var leftNav = CompileNavigationSemiJoin(leftStringCross.Join, leftIds);
                var rightNav = CompileNavigationSemiJoin(rightScalarCross2.Join, rightIds);

                rowPredicate = row => leftNav(row) && rightNav(row);
                return true;
            }

            // Cross-navigation: scalar + string on different navigations
            if (Db2NavigationQueryTranslator.TryTranslateScalarPredicate(model, left, out var leftScalarCross3) &&
                Db2NavigationQueryTranslator.TryTranslateStringPredicate(model, right, out var rightStringCross) &&
                leftScalarCross3.Join.Navigation.NavigationMember != rightStringCross.Join.Navigation.NavigationMember)
            {
                var leftIds = FindMatchingIdsScalar(tableResolver, leftScalarCross3);
                var rightIds = FindMatchingIds(tableResolver, rightStringCross);

                var leftNav = CompileNavigationSemiJoin(leftScalarCross3.Join, leftIds);
                var rightNav = CompileNavigationSemiJoin(rightStringCross.Join, rightIds);

                rowPredicate = row => leftNav(row) && rightNav(row);
                return true;
            }

            if (Db2NavigationQueryTranslator.TryTranslateStringPredicate(model, left, out var leftPlan) &&
                Db2NavigationQueryTranslator.TryTranslateStringPredicate(model, right, out var rightPlan) &&
                leftPlan.Join.Navigation.NavigationMember == rightPlan.Join.Navigation.NavigationMember)
            {
                var leftIds = FindMatchingIds(tableResolver, leftPlan);
                var rightIds = FindMatchingIds(tableResolver, rightPlan);
                leftIds.IntersectWith(rightIds);

                rowPredicate = leftPlan.Join.Kind switch
                {
                    Db2ReferenceNavigationKind.ForeignKeyToPrimaryKey => CompileForeignKeySemiJoin(leftPlan.Join.RootKeyFieldSchema, leftIds),
                    Db2ReferenceNavigationKind.SharedPrimaryKeyOneToOne => row => row is { Id: not 0 } && leftIds.Contains(row.Id),
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
                    Db2ReferenceNavigationKind.ForeignKeyToPrimaryKey => CompileForeignKeySemiJoin(navPlanLeft.Join.RootKeyFieldSchema, ids),
                    Db2ReferenceNavigationKind.SharedPrimaryKeyOneToOne => row => row is { Id: not 0 } && ids.Contains(row.Id),
                    _ => _ => false,
                };

                rowPredicate = row => nav(row) && rightRowPredicate(row);
                return true;
            }

            if (Db2NavigationQueryTranslator.TryTranslateScalarPredicate(model, left, out var scalarPlanLeft) &&
                Db2RowPredicateCompiler.TryCompile(rootFile, rootSchema, right, out var rightRowPredicate2))
            {
                var ids = FindMatchingIdsScalar(tableResolver, scalarPlanLeft);
                var nav = scalarPlanLeft.Join.Kind switch
                {
                    Db2ReferenceNavigationKind.ForeignKeyToPrimaryKey => CompileForeignKeySemiJoin(scalarPlanLeft.Join.RootKeyFieldSchema, ids),
                    Db2ReferenceNavigationKind.SharedPrimaryKeyOneToOne => row => row is { Id: not 0 } && ids.Contains(row.Id),
                    _ => _ => false,
                };

                rowPredicate = row => nav(row) && rightRowPredicate2(row);
                return true;
            }

            if (Db2NavigationQueryTranslator.TryTranslateStringPredicate(model, right, out var navPlanRight) &&
                Db2RowPredicateCompiler.TryCompile(rootFile, rootSchema, left, out var leftRowPredicate))
            {
                var ids = FindMatchingIds(tableResolver, navPlanRight);
                var nav = navPlanRight.Join.Kind switch
                {
                    Db2ReferenceNavigationKind.ForeignKeyToPrimaryKey => CompileForeignKeySemiJoin(navPlanRight.Join.RootKeyFieldSchema, ids),
                    Db2ReferenceNavigationKind.SharedPrimaryKeyOneToOne => row => row is { Id: not 0 } && ids.Contains(row.Id),
                    _ => _ => false,
                };

                rowPredicate = row => leftRowPredicate(row) && nav(row);
                return true;
            }

            if (Db2NavigationQueryTranslator.TryTranslateScalarPredicate(model, right, out var scalarPlanRight) &&
                Db2RowPredicateCompiler.TryCompile(rootFile, rootSchema, left, out var leftRowPredicate2))
            {
                var ids = FindMatchingIdsScalar(tableResolver, scalarPlanRight);
                var nav = scalarPlanRight.Join.Kind switch
                {
                    Db2ReferenceNavigationKind.ForeignKeyToPrimaryKey => CompileForeignKeySemiJoin(scalarPlanRight.Join.RootKeyFieldSchema, ids),
                    Db2ReferenceNavigationKind.SharedPrimaryKeyOneToOne => row => row is { Id: not 0 } && ids.Contains(row.Id),
                    _ => _ => false,
                };

                rowPredicate = row => leftRowPredicate2(row) && nav(row);
                return true;
            }
        }

        if (Db2NavigationQueryTranslator.TryTranslateScalarPredicate(model, predicate, out var scalarPlan))
        {
            var scalarMatchingIds = FindMatchingIdsScalar(tableResolver, scalarPlan);

            rowPredicate = scalarPlan.Join.Kind switch
            {
                Db2ReferenceNavigationKind.ForeignKeyToPrimaryKey => CompileForeignKeySemiJoin(scalarPlan.Join.RootKeyFieldSchema, scalarMatchingIds),
                Db2ReferenceNavigationKind.SharedPrimaryKeyOneToOne => row => row is { Id: not 0 } && scalarMatchingIds.Contains(row.Id),
                _ => _ => false,
            };

            return true;
        }

        if (Db2NavigationQueryTranslator.TryTranslateNullCheck(model, predicate, out var nullCheckPlan))
        {
            var existingIds = FindExistingTargetIds(tableResolver, nullCheckPlan);

            rowPredicate = nullCheckPlan.Join.Kind switch
            {
                Db2ReferenceNavigationKind.ForeignKeyToPrimaryKey => CompileNullCheckForeignKey(nullCheckPlan.Join.RootKeyFieldSchema, existingIds, nullCheckPlan.IsNotNull),
                Db2ReferenceNavigationKind.SharedPrimaryKeyOneToOne => row => existingIds.MatchesSharedPrimaryKeyNullCheck(row.Id, nullCheckPlan.IsNotNull),
                _ => _ => false,
            };

            return true;
        }

        if (!Db2NavigationQueryTranslator.TryTranslateStringPredicate(model, predicate, out var plan))
            return false;

        var stringMatchingIds = FindMatchingIds(tableResolver, plan);

        rowPredicate = plan.Join.Kind switch
        {
            Db2ReferenceNavigationKind.ForeignKeyToPrimaryKey => CompileForeignKeySemiJoin(plan.Join.RootKeyFieldSchema, stringMatchingIds),
            Db2ReferenceNavigationKind.SharedPrimaryKeyOneToOne => row => row is { Id: not 0 } && stringMatchingIds.Contains(row.Id),
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

        foreach (var relatedRow in relatedFile.EnumerateRows()
            .Where(relatedRow => relatedRow.TryGetString(relatedFieldIndex, out var s) && s == plan.Needle))
        {
            matchingIds.Add(relatedRow.Id);
        }

        return matchingIds;
    }

    private static HashSet<int> FindMatchingIdsScalar(
        Func<string, (Wdc5File File, Db2TableSchema Schema)> tableResolver,
        Db2NavigationScalarPredicatePlan plan)
    {
        return plan switch
        {
            Db2NavigationScalarPredicatePlan<bool> p => FindMatchingIdsScalar(tableResolver, p),
            Db2NavigationScalarPredicatePlan<byte> p => FindMatchingIdsScalar(tableResolver, p),
            Db2NavigationScalarPredicatePlan<sbyte> p => FindMatchingIdsScalar(tableResolver, p),
            Db2NavigationScalarPredicatePlan<short> p => FindMatchingIdsScalar(tableResolver, p),
            Db2NavigationScalarPredicatePlan<ushort> p => FindMatchingIdsScalar(tableResolver, p),
            Db2NavigationScalarPredicatePlan<int> p => FindMatchingIdsScalar(tableResolver, p),
            Db2NavigationScalarPredicatePlan<uint> p => FindMatchingIdsScalar(tableResolver, p),
            Db2NavigationScalarPredicatePlan<long> p => FindMatchingIdsScalar(tableResolver, p),
            Db2NavigationScalarPredicatePlan<ulong> p => FindMatchingIdsScalar(tableResolver, p),
            Db2NavigationScalarPredicatePlan<float> p => FindMatchingIdsScalar(tableResolver, p),
            Db2NavigationScalarPredicatePlan<double> p => FindMatchingIdsScalar(tableResolver, p),
            _ => [],
        };
    }

    private static HashSet<int> FindMatchingIdsScalar<T>(
        Func<string, (Wdc5File File, Db2TableSchema Schema)> tableResolver,
        Db2NavigationScalarPredicatePlan<T> plan) where T : unmanaged, IComparable<T>, IEquatable<T>
    {
        var (relatedFile, relatedSchema) = tableResolver(plan.Join.Target.TableName);
        if (!TryResolveRelatedScalarField(plan.Join, plan.TargetScalarMember, relatedSchema, out var relatedField))
            return [];

        var accessor = new Db2FieldAccessor(relatedField);
        HashSet<int> matchingIds = [];

        foreach (var relatedRow in relatedFile.EnumerateRows())
        {
            var value = ScalarReader<T>.Read(relatedRow, accessor);
            if (EvaluateComparison(value, plan.ComparisonValue, plan.ComparisonKind))
                matchingIds.Add(relatedRow.Id);
        }

        return matchingIds;
    }

    private static bool TryResolveRelatedScalarField(
        Db2NavigationJoinPlan join,
        MemberInfo targetScalarMember,
        Db2TableSchema relatedSchema,
        out Db2FieldSchema relatedField)
    {
        if (targetScalarMember.Name.Equals(join.Target.PrimaryKeyMember.Name, StringComparison.OrdinalIgnoreCase))
        {
            relatedField = join.Target.PrimaryKeyFieldSchema;
            return true;
        }

        return relatedSchema.TryGetFieldCaseInsensitive(targetScalarMember.Name, out relatedField!);
    }

    private static class ScalarReader<T>
    {
        public static readonly Func<Wdc5Row, Db2FieldAccessor, T> Read = Create();

        private static Func<Wdc5Row, Db2FieldAccessor, T> Create()
        {
            if (typeof(T) == typeof(bool))
                return (Func<Wdc5Row, Db2FieldAccessor, T>)(object)(Func<Wdc5Row, Db2FieldAccessor, bool>)Db2RowValue.ReadBoolean;

            if (typeof(T) == typeof(byte))
                return (Func<Wdc5Row, Db2FieldAccessor, T>)(object)(Func<Wdc5Row, Db2FieldAccessor, byte>)Db2RowValue.ReadByte;

            if (typeof(T) == typeof(sbyte))
                return (Func<Wdc5Row, Db2FieldAccessor, T>)(object)(Func<Wdc5Row, Db2FieldAccessor, sbyte>)Db2RowValue.ReadSByte;

            if (typeof(T) == typeof(short))
                return (Func<Wdc5Row, Db2FieldAccessor, T>)(object)(Func<Wdc5Row, Db2FieldAccessor, short>)Db2RowValue.ReadInt16;

            if (typeof(T) == typeof(ushort))
                return (Func<Wdc5Row, Db2FieldAccessor, T>)(object)(Func<Wdc5Row, Db2FieldAccessor, ushort>)Db2RowValue.ReadUInt16;

            if (typeof(T) == typeof(int))
                return (Func<Wdc5Row, Db2FieldAccessor, T>)(object)(Func<Wdc5Row, Db2FieldAccessor, int>)Db2RowValue.ReadInt32;

            if (typeof(T) == typeof(uint))
                return (Func<Wdc5Row, Db2FieldAccessor, T>)(object)(Func<Wdc5Row, Db2FieldAccessor, uint>)Db2RowValue.ReadUInt32;

            if (typeof(T) == typeof(long))
                return (Func<Wdc5Row, Db2FieldAccessor, T>)(object)(Func<Wdc5Row, Db2FieldAccessor, long>)Db2RowValue.ReadInt64;

            if (typeof(T) == typeof(ulong))
                return (Func<Wdc5Row, Db2FieldAccessor, T>)(object)(Func<Wdc5Row, Db2FieldAccessor, ulong>)Db2RowValue.ReadUInt64;

            if (typeof(T) == typeof(float))
                return (Func<Wdc5Row, Db2FieldAccessor, T>)(object)(Func<Wdc5Row, Db2FieldAccessor, float>)Db2RowValue.ReadSingle;

            if (typeof(T) == typeof(double))
                return (Func<Wdc5Row, Db2FieldAccessor, T>)(object)(Func<Wdc5Row, Db2FieldAccessor, double>)Db2RowValue.ReadDouble;

            throw new NotSupportedException($"Unsupported scalar type {typeof(T).FullName}.");
        }
    }

    private static HashSet<int> FindExistingTargetIds(
        Func<string, (Wdc5File File, Db2TableSchema Schema)> tableResolver,
        Db2NavigationNullCheckPlan plan)
    {
        var (relatedFile, _) = tableResolver(plan.Join.Target.TableName);
        HashSet<int> existingIds = [];

        foreach (var relatedRow in relatedFile.EnumerateRows().Where(r => r.Id != 0))
        {
            existingIds.Add(relatedRow.Id);
        }

        return existingIds;
    }

    private static bool EvaluateComparison<T>(T value, T compareValue, Db2ScalarComparisonKind kind) where T : IComparable<T>
    {
        var comparison = value.CompareTo(compareValue);

        return kind switch
        {
            Db2ScalarComparisonKind.Equal => comparison == 0,
            Db2ScalarComparisonKind.NotEqual => comparison != 0,
            Db2ScalarComparisonKind.LessThan => comparison < 0,
            Db2ScalarComparisonKind.LessThanOrEqual => comparison <= 0,
            Db2ScalarComparisonKind.GreaterThan => comparison > 0,
            Db2ScalarComparisonKind.GreaterThanOrEqual => comparison >= 0,
            _ => false,
        };
    }

    private static Func<Wdc5Row, bool> CompileNavigationSemiJoin(Db2NavigationJoinPlan join, HashSet<int> matchingIds)
    {
        return join.Kind switch
        {
            Db2ReferenceNavigationKind.ForeignKeyToPrimaryKey => CompileForeignKeySemiJoin(join.RootKeyFieldSchema, matchingIds),
            Db2ReferenceNavigationKind.SharedPrimaryKeyOneToOne => row => row is { Id: not 0 } && matchingIds.Contains(row.Id),
            _ => _ => false,
        };
    }

    private static Func<Wdc5Row, bool> CompileNullCheckForeignKey(Db2FieldSchema fkFieldSchema, HashSet<int> existingTargetIds, bool isNotNull)
    {
        if (fkFieldSchema.IsVirtual)
            throw new NotSupportedException($"Virtual foreign key field '{fkFieldSchema.Name}' is not supported for navigation predicates.");

        var rootFkIndex = fkFieldSchema.ColumnStartIndex;
        return row =>
        {
            var fk = Convert.ToInt32(row.GetScalar<long>(rootFkIndex));
            var exists = fk != 0 && existingTargetIds.Contains(fk);
            return isNotNull ? exists : !exists;
        };
    }

    private static Func<Wdc5Row, bool> CompileForeignKeySemiJoin(Db2FieldSchema fkFieldSchema, HashSet<int> ids)
    {
        if (fkFieldSchema.IsVirtual)
            throw new NotSupportedException($"Virtual foreign key field '{fkFieldSchema.Name}' is not supported for navigation predicates.");

        var rootFkIndex = fkFieldSchema.ColumnStartIndex;
        return row =>
        {
            var fk = Convert.ToInt32(row.GetScalar<long>(rootFkIndex));
            return fk != 0 && ids.Contains(fk);
        };
    }
}
