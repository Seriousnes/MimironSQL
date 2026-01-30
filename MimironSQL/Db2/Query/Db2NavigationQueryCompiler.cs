using System.Linq;
using System.Linq.Expressions;
using System.Reflection;

using MimironSQL.Db2;
using MimironSQL.Db2.Model;
using MimironSQL.Db2.Schema;
using MimironSQL.Extensions;
using MimironSQL.Formats;

namespace MimironSQL.Db2.Query;

internal static class Db2NavigationQueryCompiler
{
    public static bool TryCompileSemiJoinPredicate<TEntity, TRow>(
        Db2Model model,
        IDb2File<TRow> rootFile,
        Db2TableSchema rootSchema,
        Func<string, (IDb2File<TRow> File, Db2TableSchema Schema)> tableResolver,
        Expression<Func<TEntity, bool>> predicate,
        out Func<TRow, bool> rowPredicate)
        where TRow : struct
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

            if (Db2NavigationQueryTranslator.TryTranslateCollectionAnyPredicate(model, left, out var leftAnyPlan) &&
                Db2RowPredicateCompiler.TryCompile(rootFile, rootSchema, right, out var rightRowPredicateAny))
            {
                var ids = FindMatchingRootIdsForCollectionAny<TRow>(model, tableResolver, leftAnyPlan.Navigation, leftAnyPlan.DependentPredicate);
                var nav = CompileSharedPrimaryKeySemiJoin<TRow>(ids);
                rowPredicate = row => nav(row) && rightRowPredicateAny(row);
                return true;
            }

            if (Db2NavigationQueryTranslator.TryTranslateCollectionAnyPredicate(model, right, out var rightAnyPlan) &&
                Db2RowPredicateCompiler.TryCompile(rootFile, rootSchema, left, out var leftRowPredicateAny))
            {
                var ids = FindMatchingRootIdsForCollectionAny<TRow>(model, tableResolver, rightAnyPlan.Navigation, rightAnyPlan.DependentPredicate);
                var nav = CompileSharedPrimaryKeySemiJoin<TRow>(ids);
                rowPredicate = row => leftRowPredicateAny(row) && nav(row);
                return true;
            }

            if (Db2NavigationQueryTranslator.TryTranslateCollectionAnyPredicate(model, left, out var leftAny) &&
                Db2NavigationQueryTranslator.TryTranslateCollectionAnyPredicate(model, right, out var rightAny) &&
                leftAny.Navigation.NavigationMember != rightAny.Navigation.NavigationMember)
            {
                var leftIds = FindMatchingRootIdsForCollectionAny<TRow>(model, tableResolver, leftAny.Navigation, leftAny.DependentPredicate);
                var rightIds = FindMatchingRootIdsForCollectionAny<TRow>(model, tableResolver, rightAny.Navigation, rightAny.DependentPredicate);

                var leftNav = CompileSharedPrimaryKeySemiJoin<TRow>(leftIds);
                var rightNav = CompileSharedPrimaryKeySemiJoin<TRow>(rightIds);
                rowPredicate = row => leftNav(row) && rightNav(row);
                return true;
            }

            if (Db2NavigationQueryTranslator.TryTranslateCollectionAnyPredicate(model, left, out var leftAnySame) &&
                Db2NavigationQueryTranslator.TryTranslateCollectionAnyPredicate(model, right, out var rightAnySame) &&
                leftAnySame.Navigation.NavigationMember == rightAnySame.Navigation.NavigationMember)
            {
                var leftIds = FindMatchingRootIdsForCollectionAny<TRow>(model, tableResolver, leftAnySame.Navigation, leftAnySame.DependentPredicate);
                var rightIds = FindMatchingRootIdsForCollectionAny<TRow>(model, tableResolver, rightAnySame.Navigation, rightAnySame.DependentPredicate);
                leftIds.IntersectWith(rightIds);

                rowPredicate = CompileSharedPrimaryKeySemiJoin<TRow>(leftIds);
                return true;
            }

            // Null check + scalar predicate on same navigation
            if (Db2NavigationQueryTranslator.TryTranslateNullCheck(model, left, out var nullCheckLeft) && nullCheckLeft.IsNotNull &&
                Db2NavigationQueryTranslator.TryTranslateScalarPredicate(model, right, out var scalarRight) &&
                nullCheckLeft.Join.Navigation.NavigationMember == scalarRight.Join.Navigation.NavigationMember)
            {
                var existingIds = FindExistingTargetIds<TRow>(tableResolver, nullCheckLeft);
                var scalarIds = FindMatchingIdsScalar<TRow>(tableResolver, scalarRight);
                existingIds.IntersectWith(scalarIds);

                rowPredicate = nullCheckLeft.Join.Kind switch
                {
                    Db2ReferenceNavigationKind.ForeignKeyToPrimaryKey => CompileForeignKeySemiJoin<TRow>(nullCheckLeft.Join.RootKeyFieldSchema, existingIds),
                    Db2ReferenceNavigationKind.SharedPrimaryKeyOneToOne => CompileSharedPrimaryKeySemiJoin<TRow>(existingIds),
                    _ => _ => false,
                };

                return true;
            }

            // Scalar predicate + null check on same navigation
            if (Db2NavigationQueryTranslator.TryTranslateScalarPredicate(model, left, out var scalarLeft) &&
                Db2NavigationQueryTranslator.TryTranslateNullCheck(model, right, out var nullCheckRight) && nullCheckRight.IsNotNull &&
                scalarLeft.Join.Navigation.NavigationMember == nullCheckRight.Join.Navigation.NavigationMember)
            {
                var scalarIds = FindMatchingIdsScalar<TRow>(tableResolver, scalarLeft);
                var existingIds = FindExistingTargetIds<TRow>(tableResolver, nullCheckRight);
                scalarIds.IntersectWith(existingIds);

                rowPredicate = scalarLeft.Join.Kind switch
                {
                    Db2ReferenceNavigationKind.ForeignKeyToPrimaryKey => CompileForeignKeySemiJoin<TRow>(scalarLeft.Join.RootKeyFieldSchema, scalarIds),
                    Db2ReferenceNavigationKind.SharedPrimaryKeyOneToOne => CompileSharedPrimaryKeySemiJoin<TRow>(scalarIds),
                    _ => _ => false,
                };

                return true;
            }

            // String + scalar predicates on same navigation (intersection)
            if (Db2NavigationQueryTranslator.TryTranslateStringPredicate(model, left, out var leftStringScalar) &&
                Db2NavigationQueryTranslator.TryTranslateScalarPredicate(model, right, out var rightScalarString) &&
                leftStringScalar.Join.Navigation.NavigationMember == rightScalarString.Join.Navigation.NavigationMember)
            {
                var leftIds = FindMatchingIds<TRow>(tableResolver, leftStringScalar);
                var rightIds = FindMatchingIdsScalar<TRow>(tableResolver, rightScalarString);
                leftIds.IntersectWith(rightIds);

                rowPredicate = leftStringScalar.Join.Kind switch
                {
                    Db2ReferenceNavigationKind.ForeignKeyToPrimaryKey => CompileForeignKeySemiJoin<TRow>(leftStringScalar.Join.RootKeyFieldSchema, leftIds),
                    Db2ReferenceNavigationKind.SharedPrimaryKeyOneToOne => CompileSharedPrimaryKeySemiJoin<TRow>(leftIds),
                    _ => _ => false,
                };

                return true;
            }

            // Scalar + string predicates on same navigation (intersection)
            if (Db2NavigationQueryTranslator.TryTranslateScalarPredicate(model, left, out var leftScalarString2) &&
                Db2NavigationQueryTranslator.TryTranslateStringPredicate(model, right, out var rightStringScalar2) &&
                leftScalarString2.Join.Navigation.NavigationMember == rightStringScalar2.Join.Navigation.NavigationMember)
            {
                var leftIds = FindMatchingIdsScalar<TRow>(tableResolver, leftScalarString2);
                var rightIds = FindMatchingIds<TRow>(tableResolver, rightStringScalar2);
                leftIds.IntersectWith(rightIds);

                rowPredicate = leftScalarString2.Join.Kind switch
                {
                    Db2ReferenceNavigationKind.ForeignKeyToPrimaryKey => CompileForeignKeySemiJoin<TRow>(leftScalarString2.Join.RootKeyFieldSchema, leftIds),
                    Db2ReferenceNavigationKind.SharedPrimaryKeyOneToOne => CompileSharedPrimaryKeySemiJoin<TRow>(leftIds),
                    _ => _ => false,
                };

                return true;
            }

            // Two scalar predicates on same navigation
            if (Db2NavigationQueryTranslator.TryTranslateScalarPredicate(model, left, out var leftScalarPlan) &&
                Db2NavigationQueryTranslator.TryTranslateScalarPredicate(model, right, out var rightScalarPlan) &&
                leftScalarPlan.Join.Navigation.NavigationMember == rightScalarPlan.Join.Navigation.NavigationMember)
            {
                var leftIds = FindMatchingIdsScalar<TRow>(tableResolver, leftScalarPlan);
                var rightIds = FindMatchingIdsScalar<TRow>(tableResolver, rightScalarPlan);
                leftIds.IntersectWith(rightIds);

                rowPredicate = leftScalarPlan.Join.Kind switch
                {
                    Db2ReferenceNavigationKind.ForeignKeyToPrimaryKey => CompileForeignKeySemiJoin<TRow>(leftScalarPlan.Join.RootKeyFieldSchema, leftIds),
                    Db2ReferenceNavigationKind.SharedPrimaryKeyOneToOne => CompileSharedPrimaryKeySemiJoin<TRow>(leftIds),
                    _ => _ => false,
                };

                return true;
            }

            // Cross-navigation: scalar + scalar on different navigations
            if (Db2NavigationQueryTranslator.TryTranslateScalarPredicate(model, left, out var leftScalarCross) &&
                Db2NavigationQueryTranslator.TryTranslateScalarPredicate(model, right, out var rightScalarCross) &&
                leftScalarCross.Join.Navigation.NavigationMember != rightScalarCross.Join.Navigation.NavigationMember)
            {
                var leftIds = FindMatchingIdsScalar<TRow>(tableResolver, leftScalarCross);
                var rightIds = FindMatchingIdsScalar<TRow>(tableResolver, rightScalarCross);

                var leftNav = CompileNavigationSemiJoin<TRow>(leftScalarCross.Join, leftIds);
                var rightNav = CompileNavigationSemiJoin<TRow>(rightScalarCross.Join, rightIds);

                rowPredicate = row => leftNav(row) && rightNav(row);
                return true;
            }

            // Cross-navigation: string + scalar on different navigations  
            if (Db2NavigationQueryTranslator.TryTranslateStringPredicate(model, left, out var leftStringCross) &&
                Db2NavigationQueryTranslator.TryTranslateScalarPredicate(model, right, out var rightScalarCross2) &&
                leftStringCross.Join.Navigation.NavigationMember != rightScalarCross2.Join.Navigation.NavigationMember)
            {
                var leftIds = FindMatchingIds<TRow>(tableResolver, leftStringCross);
                var rightIds = FindMatchingIdsScalar<TRow>(tableResolver, rightScalarCross2);

                var leftNav = CompileNavigationSemiJoin<TRow>(leftStringCross.Join, leftIds);
                var rightNav = CompileNavigationSemiJoin<TRow>(rightScalarCross2.Join, rightIds);

                rowPredicate = row => leftNav(row) && rightNav(row);
                return true;
            }

            // Cross-navigation: scalar + string on different navigations
            if (Db2NavigationQueryTranslator.TryTranslateScalarPredicate(model, left, out var leftScalarCross3) &&
                Db2NavigationQueryTranslator.TryTranslateStringPredicate(model, right, out var rightStringCross) &&
                leftScalarCross3.Join.Navigation.NavigationMember != rightStringCross.Join.Navigation.NavigationMember)
            {
                var leftIds = FindMatchingIdsScalar<TRow>(tableResolver, leftScalarCross3);
                var rightIds = FindMatchingIds<TRow>(tableResolver, rightStringCross);

                var leftNav = CompileNavigationSemiJoin<TRow>(leftScalarCross3.Join, leftIds);
                var rightNav = CompileNavigationSemiJoin<TRow>(rightStringCross.Join, rightIds);

                rowPredicate = row => leftNav(row) && rightNav(row);
                return true;
            }

            if (Db2NavigationQueryTranslator.TryTranslateStringPredicate(model, left, out var leftPlan) &&
                Db2NavigationQueryTranslator.TryTranslateStringPredicate(model, right, out var rightPlan) &&
                leftPlan.Join.Navigation.NavigationMember == rightPlan.Join.Navigation.NavigationMember)
            {
                var leftIds = FindMatchingIds<TRow>(tableResolver, leftPlan);
                var rightIds = FindMatchingIds<TRow>(tableResolver, rightPlan);
                leftIds.IntersectWith(rightIds);

                rowPredicate = leftPlan.Join.Kind switch
                {
                    Db2ReferenceNavigationKind.ForeignKeyToPrimaryKey => CompileForeignKeySemiJoin<TRow>(leftPlan.Join.RootKeyFieldSchema, leftIds),
                    Db2ReferenceNavigationKind.SharedPrimaryKeyOneToOne => CompileSharedPrimaryKeySemiJoin<TRow>(leftIds),
                    _ => _ => false,
                };

                return true;
            }

            if (Db2NavigationQueryTranslator.TryTranslateStringPredicate(model, left, out var navPlanLeft) &&
                Db2RowPredicateCompiler.TryCompile(rootFile, rootSchema, right, out var rightRowPredicate))
            {
                var ids = FindMatchingIds<TRow>(tableResolver, navPlanLeft);
                var nav = navPlanLeft.Join.Kind switch
                {
                    Db2ReferenceNavigationKind.ForeignKeyToPrimaryKey => CompileForeignKeySemiJoin<TRow>(navPlanLeft.Join.RootKeyFieldSchema, ids),
                    Db2ReferenceNavigationKind.SharedPrimaryKeyOneToOne => CompileSharedPrimaryKeySemiJoin<TRow>(ids),
                    _ => _ => false,
                };

                rowPredicate = row => nav(row) && rightRowPredicate(row);
                return true;
            }

            if (Db2NavigationQueryTranslator.TryTranslateScalarPredicate(model, left, out var scalarPlanLeft) &&
                Db2RowPredicateCompiler.TryCompile(rootFile, rootSchema, right, out var rightRowPredicate2))
            {
                var ids = FindMatchingIdsScalar<TRow>(tableResolver, scalarPlanLeft);
                var nav = scalarPlanLeft.Join.Kind switch
                {
                    Db2ReferenceNavigationKind.ForeignKeyToPrimaryKey => CompileForeignKeySemiJoin<TRow>(scalarPlanLeft.Join.RootKeyFieldSchema, ids),
                    Db2ReferenceNavigationKind.SharedPrimaryKeyOneToOne => CompileSharedPrimaryKeySemiJoin<TRow>(ids),
                    _ => _ => false,
                };

                rowPredicate = row => nav(row) && rightRowPredicate2(row);
                return true;
            }

            if (Db2NavigationQueryTranslator.TryTranslateStringPredicate(model, right, out var navPlanRight) &&
                Db2RowPredicateCompiler.TryCompile(rootFile, rootSchema, left, out var leftRowPredicate))
            {
                var ids = FindMatchingIds<TRow>(tableResolver, navPlanRight);
                var nav = navPlanRight.Join.Kind switch
                {
                    Db2ReferenceNavigationKind.ForeignKeyToPrimaryKey => CompileForeignKeySemiJoin<TRow>(navPlanRight.Join.RootKeyFieldSchema, ids),
                    Db2ReferenceNavigationKind.SharedPrimaryKeyOneToOne => CompileSharedPrimaryKeySemiJoin<TRow>(ids),
                    _ => _ => false,
                };

                rowPredicate = row => leftRowPredicate(row) && nav(row);
                return true;
            }

            if (Db2NavigationQueryTranslator.TryTranslateScalarPredicate(model, right, out var scalarPlanRight) &&
                Db2RowPredicateCompiler.TryCompile(rootFile, rootSchema, left, out var leftRowPredicate2))
            {
                var ids = FindMatchingIdsScalar<TRow>(tableResolver, scalarPlanRight);
                var nav = scalarPlanRight.Join.Kind switch
                {
                    Db2ReferenceNavigationKind.ForeignKeyToPrimaryKey => CompileForeignKeySemiJoin<TRow>(scalarPlanRight.Join.RootKeyFieldSchema, ids),
                    Db2ReferenceNavigationKind.SharedPrimaryKeyOneToOne => CompileSharedPrimaryKeySemiJoin<TRow>(ids),
                    _ => _ => false,
                };

                rowPredicate = row => leftRowPredicate2(row) && nav(row);
                return true;
            }
        }

        if (Db2NavigationQueryTranslator.TryTranslateCollectionAnyPredicate(model, predicate, out var anyPlan))
        {
            var ids = FindMatchingRootIdsForCollectionAny<TRow>(model, tableResolver, anyPlan.Navigation, anyPlan.DependentPredicate);
            rowPredicate = CompileSharedPrimaryKeySemiJoin<TRow>(ids);
            return true;
        }

        if (Db2NavigationQueryTranslator.TryTranslateScalarPredicate(model, predicate, out var scalarPlan))
        {
            var scalarMatchingIds = FindMatchingIdsScalar<TRow>(tableResolver, scalarPlan);

            rowPredicate = scalarPlan.Join.Kind switch
            {
                Db2ReferenceNavigationKind.ForeignKeyToPrimaryKey => CompileForeignKeySemiJoin<TRow>(scalarPlan.Join.RootKeyFieldSchema, scalarMatchingIds),
                Db2ReferenceNavigationKind.SharedPrimaryKeyOneToOne => CompileSharedPrimaryKeySemiJoin<TRow>(scalarMatchingIds),
                _ => _ => false,
            };

            return true;
        }

        if (Db2NavigationQueryTranslator.TryTranslateNullCheck(model, predicate, out var nullCheckPlan))
        {
            var existingIds = FindExistingTargetIds<TRow>(tableResolver, nullCheckPlan);

            rowPredicate = nullCheckPlan.Join.Kind switch
            {
                Db2ReferenceNavigationKind.ForeignKeyToPrimaryKey => CompileNullCheckForeignKey<TRow>(nullCheckPlan.Join.RootKeyFieldSchema, existingIds, nullCheckPlan.IsNotNull),
                Db2ReferenceNavigationKind.SharedPrimaryKeyOneToOne => row => existingIds.MatchesSharedPrimaryKeyNullCheck(row.Get<int>(Db2VirtualFieldIndex.Id), nullCheckPlan.IsNotNull),
                _ => _ => false,
            };

            return true;
        }

        if (!Db2NavigationQueryTranslator.TryTranslateStringPredicate(model, predicate, out var plan))
            return false;

        var stringMatchingIds = FindMatchingIds<TRow>(tableResolver, plan);

        rowPredicate = plan.Join.Kind switch
        {
            Db2ReferenceNavigationKind.ForeignKeyToPrimaryKey => CompileForeignKeySemiJoin<TRow>(plan.Join.RootKeyFieldSchema, stringMatchingIds),
            Db2ReferenceNavigationKind.SharedPrimaryKeyOneToOne => CompileSharedPrimaryKeySemiJoin<TRow>(stringMatchingIds),
            _ => _ => false,
        };

        return true;
    }

    private static HashSet<int> FindMatchingRootIdsForCollectionAny<TRow>(
        Db2Model model,
        Func<string, (IDb2File<TRow> File, Db2TableSchema Schema)> tableResolver,
        Db2CollectionNavigation navigation,
        LambdaExpression? dependentPredicate)
        where TRow : struct
    {
        if (navigation.Kind != Db2CollectionNavigationKind.DependentForeignKeyToPrimaryKey)
            return [];

        if (navigation.DependentForeignKeyFieldSchema is null)
            return [];

        var m = typeof(Db2NavigationQueryCompiler)
            .GetMethod(nameof(FindMatchingRootIdsForCollectionAnyTyped), BindingFlags.Static | BindingFlags.NonPublic)!;

        var g = m.MakeGenericMethod(navigation.TargetClrType, typeof(TRow));

        var typedPredicate = dependentPredicate is null
            ? null
            : (LambdaExpression)Expression.Lambda(
                typeof(Func<,>).MakeGenericType(navigation.TargetClrType, typeof(bool)),
                dependentPredicate.Body,
                dependentPredicate.Parameters);

        return (HashSet<int>)g.Invoke(null, [model, tableResolver, navigation, typedPredicate])!;
    }

    private static HashSet<int> FindMatchingRootIdsForCollectionAnyTyped<TDependent, TRow>(
        Db2Model model,
        Func<string, (IDb2File<TRow> File, Db2TableSchema Schema)> tableResolver,
        Db2CollectionNavigation navigation,
        LambdaExpression? dependentPredicate)
        where TDependent : class
        where TRow : struct
    {
        if (navigation.Kind != Db2CollectionNavigationKind.DependentForeignKeyToPrimaryKey)
            return [];

        if (navigation.DependentForeignKeyFieldSchema is null)
            return [];

        var dependentEntityType = model.GetEntityType(typeof(TDependent));
        var (dependentFile, dependentSchema) = tableResolver(dependentEntityType.TableName);

        var fkIndex = navigation.DependentForeignKeyFieldSchema.Value.ColumnStartIndex;
        HashSet<int> matchingRootIds = [];

        if (dependentPredicate is null)
        {
            foreach (var row in dependentFile.EnumerateRows())
            {
                var fk = row.Get<int>(fkIndex);
                if (fk != 0)
                    matchingRootIds.Add(fk);
            }

            return matchingRootIds;
        }

        var typedPredicate = (Expression<Func<TDependent, bool>>)dependentPredicate;

        if (Db2RowPredicateCompiler.TryCompile(dependentFile, dependentSchema, typedPredicate, out var dependentRowPredicate))
        {
            foreach (var row in dependentFile.EnumerateRows())
            {
                if (!dependentRowPredicate(row))
                    continue;

                var fk = row.Get<int>(fkIndex);
                if (fk != 0)
                    matchingRootIds.Add(fk);
            }

            return matchingRootIds;
        }

        var dependentEntityPredicate = typedPredicate.Compile();
        var materializer = new Db2EntityMaterializer<TDependent, TRow>(dependentSchema);

        foreach (var row in dependentFile.EnumerateRows())
        {
            var rowId = row.Get<int>(Db2VirtualFieldIndex.Id);
            if (!dependentFile.TryGetRowHandle(rowId, out var handle))
                continue;

            var entity = materializer.Materialize(dependentFile, handle);
            if (!dependentEntityPredicate(entity))
                continue;

            var fk = row.Get<int>(fkIndex);
            if (fk != 0)
                matchingRootIds.Add(fk);
        }

        return matchingRootIds;
    }

    private static HashSet<int> FindMatchingIds<TRow>(
        Func<string, (IDb2File<TRow> File, Db2TableSchema Schema)> tableResolver,
        Db2NavigationStringPredicatePlan plan)
        where TRow : struct
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

            var isDenseOptimizable = !relatedFile.Flags.HasFlag(Db2Flags.Sparse) && !relatedField.IsVirtual;
            if (isDenseOptimizable && relatedFile is IDb2DenseStringTableIndexProvider<TRow> provider)
            {
                var starts = Db2DenseStringScanner.FindStartOffsets(relatedFile.DenseStringTableBytes.Span, plan.Needle, mk);

                foreach (var relatedRow in relatedFile.EnumerateRows())
                {
                    var ok = mk switch
                    {
                        Db2StringMatchKind.Contains => Db2DenseStringMatch.Contains(provider, relatedRow, relatedFieldIndex, starts),
                        Db2StringMatchKind.StartsWith => Db2DenseStringMatch.StartsWith(provider, relatedRow, relatedFieldIndex, starts),
                        Db2StringMatchKind.EndsWith => Db2DenseStringMatch.EndsWith(provider, relatedRow, relatedFieldIndex, starts),
                        _ => false,
                    };

                    if (!ok)
                        continue;

                    matchingIds.Add(relatedRow.Get<int>(Db2VirtualFieldIndex.Id));
                }

                return matchingIds;
            }

            foreach (var relatedRow in relatedFile.EnumerateRows())
            {
                var s = relatedRow.Get<string>(relatedFieldIndex);

                var ok = plan.MatchKind switch
                {
                    Db2NavigationStringMatchKind.Contains => s.Contains(plan.Needle, StringComparison.Ordinal),
                    Db2NavigationStringMatchKind.StartsWith => s.StartsWith(plan.Needle, StringComparison.Ordinal),
                    Db2NavigationStringMatchKind.EndsWith => s.EndsWith(plan.Needle, StringComparison.Ordinal),
                    _ => false,
                };

                if (ok)
                    matchingIds.Add(relatedRow.Get<int>(Db2VirtualFieldIndex.Id));
            }

            return matchingIds;
        }

        foreach (var relatedRow in relatedFile.EnumerateRows())
        {
            var s = relatedRow.Get<string>(relatedFieldIndex);
            if (s == plan.Needle)
                matchingIds.Add(relatedRow.Get<int>(Db2VirtualFieldIndex.Id));
        }

        return matchingIds;
    }

    private static HashSet<int> FindMatchingIdsScalar<TRow>(
        Func<string, (IDb2File<TRow> File, Db2TableSchema Schema)> tableResolver,
        Db2NavigationScalarPredicatePlan plan)
        where TRow : struct
    {
        return plan switch
        {
            Db2NavigationScalarPredicatePlan<bool> p => FindMatchingIdsScalar<TRow, bool>(tableResolver, p),
            Db2NavigationScalarPredicatePlan<byte> p => FindMatchingIdsScalar<TRow, byte>(tableResolver, p),
            Db2NavigationScalarPredicatePlan<sbyte> p => FindMatchingIdsScalar<TRow, sbyte>(tableResolver, p),
            Db2NavigationScalarPredicatePlan<short> p => FindMatchingIdsScalar<TRow, short>(tableResolver, p),
            Db2NavigationScalarPredicatePlan<ushort> p => FindMatchingIdsScalar<TRow, ushort>(tableResolver, p),
            Db2NavigationScalarPredicatePlan<int> p => FindMatchingIdsScalar<TRow, int>(tableResolver, p),
            Db2NavigationScalarPredicatePlan<uint> p => FindMatchingIdsScalar<TRow, uint>(tableResolver, p),
            Db2NavigationScalarPredicatePlan<long> p => FindMatchingIdsScalar<TRow, long>(tableResolver, p),
            Db2NavigationScalarPredicatePlan<ulong> p => FindMatchingIdsScalar<TRow, ulong>(tableResolver, p),
            Db2NavigationScalarPredicatePlan<float> p => FindMatchingIdsScalar<TRow, float>(tableResolver, p),
            Db2NavigationScalarPredicatePlan<double> p => FindMatchingIdsScalar<TRow, double>(tableResolver, p),
            _ => [],
        };
    }

    private static HashSet<int> FindMatchingIdsScalar<TRow, T>(
        Func<string, (IDb2File<TRow> File, Db2TableSchema Schema)> tableResolver,
        Db2NavigationScalarPredicatePlan<T> plan)
        where TRow : struct
        where T : unmanaged, IComparable<T>, IEquatable<T>
    {
        var (relatedFile, relatedSchema) = tableResolver(plan.Join.Target.TableName);
        if (!TryResolveRelatedScalarField(plan.Join, plan.TargetScalarMember, relatedSchema, out var relatedField))
            return [];

        var relatedFieldIndex = relatedField.ColumnStartIndex;
        HashSet<int> matchingIds = [];

        foreach (var relatedRow in relatedFile.EnumerateRows())
        {
            var value = relatedRow.Get<T>(relatedFieldIndex);
            if (EvaluateComparison(value, plan.ComparisonValue, plan.ComparisonKind))
                matchingIds.Add(relatedRow.Get<int>(Db2VirtualFieldIndex.Id));
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

    private static HashSet<int> FindExistingTargetIds<TRow>(
        Func<string, (IDb2File<TRow> File, Db2TableSchema Schema)> tableResolver,
        Db2NavigationNullCheckPlan plan)
        where TRow : struct
    {
        var (relatedFile, _) = tableResolver(plan.Join.Target.TableName);
        HashSet<int> existingIds = [];

        foreach (var relatedRow in relatedFile.EnumerateRows())
        {
            var id = relatedRow.Get<int>(Db2VirtualFieldIndex.Id);
            if (id != 0)
                existingIds.Add(id);
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

    private static Func<TRow, bool> CompileNavigationSemiJoin<TRow>(Db2NavigationJoinPlan join, HashSet<int> matchingIds)
        where TRow : struct
    {
        return join.Kind switch
        {
            Db2ReferenceNavigationKind.ForeignKeyToPrimaryKey => CompileForeignKeySemiJoin<TRow>(join.RootKeyFieldSchema, matchingIds),
            Db2ReferenceNavigationKind.SharedPrimaryKeyOneToOne => CompileSharedPrimaryKeySemiJoin<TRow>(matchingIds),
            _ => _ => false,
        };
    }

    private static Func<TRow, bool> CompileNullCheckForeignKey<TRow>(Db2FieldSchema fkFieldSchema, HashSet<int> existingTargetIds, bool isNotNull)
        where TRow : struct
    {
        if (fkFieldSchema.IsVirtual)
            throw new NotSupportedException($"Virtual foreign key field '{fkFieldSchema.Name}' is not supported for navigation predicates.");

        var rootFkIndex = fkFieldSchema.ColumnStartIndex;
        return row =>
        {
            var fk = row.Get<int>(rootFkIndex);
            var exists = fk != 0 && existingTargetIds.Contains(fk);
            return isNotNull ? exists : !exists;
        };
    }

    private static Func<TRow, bool> CompileForeignKeySemiJoin<TRow>(Db2FieldSchema fkFieldSchema, HashSet<int> ids)
        where TRow : struct
    {
        if (fkFieldSchema.IsVirtual)
            throw new NotSupportedException($"Virtual foreign key field '{fkFieldSchema.Name}' is not supported for navigation predicates.");

        var rootFkIndex = fkFieldSchema.ColumnStartIndex;
        return row =>
        {
            var fk = row.Get<int>(rootFkIndex);
            return fk != 0 && ids.Contains(fk);
        };
    }

    private static Func<TRow, bool> CompileSharedPrimaryKeySemiJoin<TRow>(HashSet<int> matchingIds)
        where TRow : struct
        => row =>
        {
            var id = row.Get<int>(Db2VirtualFieldIndex.Id);
            return id != 0 && matchingIds.Contains(id);
        };
}
