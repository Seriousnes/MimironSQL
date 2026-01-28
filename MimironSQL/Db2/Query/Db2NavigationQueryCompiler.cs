using System.Linq;
using System.Linq.Expressions;
using System.Reflection;

using MimironSQL.Db2;
using MimironSQL.Db2.Model;
using MimironSQL.Db2.Schema;
using MimironSQL.Db2.Wdc5;
using MimironSQL.Formats;

namespace MimironSQL.Db2.Query;

internal static class Db2NavigationQueryCompiler
{
    public static bool TryCompileSemiJoinPredicate<TEntity>(
        Db2Model model,
        Wdc5File rootFile,
        Db2TableSchema rootSchema,
        Func<string, (IDb2File File, Db2TableSchema Schema)> tableResolver,
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
                Db2ReferenceNavigationKind.SharedPrimaryKeyOneToOne => row => nullCheckPlan.IsNotNull ? (row is { Id: not 0 } && existingIds.Contains(row.Id)) : (row.Id == 0 || !existingIds.Contains(row.Id)),
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
        Func<string, (IDb2File File, Db2TableSchema Schema)> tableResolver,
        Db2NavigationStringPredicatePlan plan)
    {
        var (relatedFileHandle, relatedSchema) = tableResolver(plan.Join.Target.TableName);
        var relatedFile = (Wdc5File)relatedFileHandle;

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
        Func<string, (IDb2File File, Db2TableSchema Schema)> tableResolver,
        Db2NavigationScalarPredicatePlan plan)
    {
        var (relatedFileHandle, relatedSchema) = tableResolver(plan.Join.Target.TableName);
        var relatedFile = (Wdc5File)relatedFileHandle;

        Db2FieldSchema relatedField;
        if (plan.TargetScalarMember.Name.Equals(plan.Join.Target.PrimaryKeyMember.Name, StringComparison.OrdinalIgnoreCase))
        {
            relatedField = plan.Join.Target.PrimaryKeyFieldSchema;
        }
        else if (!relatedSchema.TryGetFieldCaseInsensitive(plan.TargetScalarMember.Name, out relatedField!))
        {
            return [];
        }

        var accessor = new Db2FieldAccessor(relatedField);
        HashSet<int> matchingIds = [];

        foreach (var relatedRow in relatedFile.EnumerateRows().Where(r => EvaluateScalarComparison(r, accessor, plan.ComparisonKind, plan.ComparisonValue, plan.ScalarType)))
        {
            matchingIds.Add(relatedRow.Id);
        }

        return matchingIds;
    }

    private static HashSet<int> FindExistingTargetIds(
        Func<string, (IDb2File File, Db2TableSchema Schema)> tableResolver,
        Db2NavigationNullCheckPlan plan)
    {
        var (relatedFileHandle, _) = tableResolver(plan.Join.Target.TableName);
        var relatedFile = (Wdc5File)relatedFileHandle;
        HashSet<int> existingIds = [];

        foreach (var relatedRow in relatedFile.EnumerateRows().Where(r => r.Id != 0))
        {
            existingIds.Add(relatedRow.Id);
        }

        return existingIds;
    }

    private static bool EvaluateScalarComparison(Wdc5Row row, Db2FieldAccessor accessor, Db2ScalarComparisonKind kind, object compareValue, Type scalarType)
    {
        object? value = ReadScalarValue(row, accessor, scalarType);
        if (value is null)
            return false;

        var comparison = CompareScalars(value, compareValue, scalarType);

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

    private static object? ReadScalarValue(Wdc5Row row, Db2FieldAccessor accessor, Type type)
    {
        return Type.GetTypeCode(type) switch
        {
            TypeCode.Byte => Db2RowValue.Read<byte>(row, accessor),
            TypeCode.SByte => Db2RowValue.Read<sbyte>(row, accessor),
            TypeCode.Int16 => Db2RowValue.Read<short>(row, accessor),
            TypeCode.UInt16 => Db2RowValue.Read<ushort>(row, accessor),
            TypeCode.Int32 => Db2RowValue.Read<int>(row, accessor),
            TypeCode.UInt32 => Db2RowValue.Read<uint>(row, accessor),
            TypeCode.Int64 => Db2RowValue.Read<long>(row, accessor),
            TypeCode.UInt64 => Db2RowValue.Read<ulong>(row, accessor),
            TypeCode.Single => Db2RowValue.Read<float>(row, accessor),
            TypeCode.Double => Db2RowValue.Read<double>(row, accessor),
            TypeCode.Boolean => Db2RowValue.Read<bool>(row, accessor),
            _ => type.IsEnum ? Db2RowValue.Read<int>(row, accessor) : null,
        };
    }

    private static int CompareScalars(object a, object b, Type type)
    {
        return Type.GetTypeCode(type) switch
        {
            TypeCode.Byte => ((byte)a).CompareTo((byte)b),
            TypeCode.SByte => ((sbyte)a).CompareTo((sbyte)b),
            TypeCode.Int16 => ((short)a).CompareTo((short)b),
            TypeCode.UInt16 => ((ushort)a).CompareTo((ushort)b),
            TypeCode.Int32 => ((int)a).CompareTo((int)b),
            TypeCode.UInt32 => ((uint)a).CompareTo((uint)b),
            TypeCode.Int64 => ((long)a).CompareTo((long)b),
            TypeCode.UInt64 => ((ulong)a).CompareTo((ulong)b),
            TypeCode.Single => ((float)a).CompareTo((float)b),
            TypeCode.Double => ((double)a).CompareTo((double)b),
            TypeCode.Boolean => ((bool)a).CompareTo((bool)b),
            _ => type.IsEnum
                ? Comparer<int>.Default.Compare(Convert.ToInt32(a), Convert.ToInt32(b))
                : throw new NotSupportedException($"Comparison not supported for type '{type.FullName}'."),
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
