using System.Linq.Expressions;
using System.Reflection;

using MimironSQL.Db2;
using MimironSQL.EntityFrameworkCore.Db2.Model;
using MimironSQL.EntityFrameworkCore.Db2.Schema;
using MimironSQL.EntityFrameworkCore.Extensions;
using MimironSQL.Formats;

namespace MimironSQL.EntityFrameworkCore.Db2.Query;

internal static class Db2NavigationQueryCompiler
{
    public static bool TryCompileSemiJoinPredicate<TEntity, TRow>(
        Db2Model model,
        IDb2File<TRow> rootFile,        
        Func<string, (IDb2File<TRow> File, Db2TableSchema Schema)> tableResolver,
        Expression<Func<TEntity, bool>> predicate,
        out Func<TRow, bool> rowPredicate)
        where TRow : struct, IRowHandle
    {
        rowPredicate = _ => true;

        predicate = RewriteCollectionCountComparisonsToAny(model, predicate);

        var rootEntityType = model.GetEntityType(typeof(TEntity));

        var body = predicate.Body is UnaryExpression { NodeType: ExpressionType.Convert or ExpressionType.ConvertChecked } u
            ? u.Operand
            : predicate.Body;

        if (body is BinaryExpression { NodeType: ExpressionType.AndAlso } andAlso)
        {
            var p = predicate.Parameters[0];
            var left = Expression.Lambda<Func<TEntity, bool>>(andAlso.Left, p);
            var right = Expression.Lambda<Func<TEntity, bool>>(andAlso.Right, p);

            if (Db2NavigationQueryTranslator.TryTranslateCollectionAnyPredicate(model, left, out var leftAnyPlan) &&
                Db2RowPredicateCompiler.TryCompile(rootFile, rootEntityType, right, out var rightRowPredicateAny))
            {
                var ids = FindMatchingRootIdsForCollectionAny<TRow>(model, tableResolver, leftAnyPlan.Navigation, leftAnyPlan.DependentPredicate);
                var nav = CompileSharedPrimaryKeySemiJoin<TRow>(ids);
                rowPredicate = row => nav(row) && rightRowPredicateAny(row);
                return true;
            }

            if (Db2NavigationQueryTranslator.TryTranslateCollectionAnyPredicate(model, right, out var rightAnyPlan) &&
                Db2RowPredicateCompiler.TryCompile(rootFile, rootEntityType, left, out var leftRowPredicateAny))
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
                    Db2ReferenceNavigationKind.ForeignKeyToPrimaryKey => CompileForeignKeySemiJoin<TRow>(rootFile, nullCheckLeft.Join.RootKeyFieldSchema, existingIds),
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
                    Db2ReferenceNavigationKind.ForeignKeyToPrimaryKey => CompileForeignKeySemiJoin<TRow>(rootFile, scalarLeft.Join.RootKeyFieldSchema, scalarIds),
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
                    Db2ReferenceNavigationKind.ForeignKeyToPrimaryKey => CompileForeignKeySemiJoin<TRow>(rootFile, leftStringScalar.Join.RootKeyFieldSchema, leftIds),
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
                    Db2ReferenceNavigationKind.ForeignKeyToPrimaryKey => CompileForeignKeySemiJoin<TRow>(rootFile, leftScalarString2.Join.RootKeyFieldSchema, leftIds),
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
                    Db2ReferenceNavigationKind.ForeignKeyToPrimaryKey => CompileForeignKeySemiJoin<TRow>(rootFile, leftScalarPlan.Join.RootKeyFieldSchema, leftIds),
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

                var leftNav = CompileNavigationSemiJoin<TRow>(rootFile, leftScalarCross.Join, leftIds);
                var rightNav = CompileNavigationSemiJoin<TRow>(rootFile, rightScalarCross.Join, rightIds);

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

                var leftNav = CompileNavigationSemiJoin<TRow>(rootFile, leftStringCross.Join, leftIds);
                var rightNav = CompileNavigationSemiJoin<TRow>(rootFile, rightScalarCross2.Join, rightIds);

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

                var leftNav = CompileNavigationSemiJoin<TRow>(rootFile, leftScalarCross3.Join, leftIds);
                var rightNav = CompileNavigationSemiJoin<TRow>(rootFile, rightStringCross.Join, rightIds);

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
                    Db2ReferenceNavigationKind.ForeignKeyToPrimaryKey => CompileForeignKeySemiJoin<TRow>(rootFile, leftPlan.Join.RootKeyFieldSchema, leftIds),
                    Db2ReferenceNavigationKind.SharedPrimaryKeyOneToOne => CompileSharedPrimaryKeySemiJoin<TRow>(leftIds),
                    _ => _ => false,
                };

                return true;
            }

            if (Db2NavigationQueryTranslator.TryTranslateStringPredicate(model, left, out var navPlanLeft) &&
                Db2RowPredicateCompiler.TryCompile(rootFile, rootEntityType, right, out var rightRowPredicate))
            {
                var ids = FindMatchingIds<TRow>(tableResolver, navPlanLeft);
                var nav = navPlanLeft.Join.Kind switch
                {
                    Db2ReferenceNavigationKind.ForeignKeyToPrimaryKey => CompileForeignKeySemiJoin<TRow>(rootFile, navPlanLeft.Join.RootKeyFieldSchema, ids),
                    Db2ReferenceNavigationKind.SharedPrimaryKeyOneToOne => CompileSharedPrimaryKeySemiJoin<TRow>(ids),
                    _ => _ => false,
                };

                rowPredicate = row => nav(row) && rightRowPredicate(row);
                return true;
            }

            if (Db2NavigationQueryTranslator.TryTranslateScalarPredicate(model, left, out var scalarPlanLeft) &&
                Db2RowPredicateCompiler.TryCompile(rootFile, rootEntityType, right, out var rightRowPredicate2))
            {
                var ids = FindMatchingIdsScalar<TRow>(tableResolver, scalarPlanLeft);
                var nav = scalarPlanLeft.Join.Kind switch
                {
                    Db2ReferenceNavigationKind.ForeignKeyToPrimaryKey => CompileForeignKeySemiJoin<TRow>(rootFile, scalarPlanLeft.Join.RootKeyFieldSchema, ids),
                    Db2ReferenceNavigationKind.SharedPrimaryKeyOneToOne => CompileSharedPrimaryKeySemiJoin<TRow>(ids),
                    _ => _ => false,
                };

                rowPredicate = row => nav(row) && rightRowPredicate2(row);
                return true;
            }

            if (Db2NavigationQueryTranslator.TryTranslateStringPredicate(model, right, out var navPlanRight) &&
                Db2RowPredicateCompiler.TryCompile(rootFile, rootEntityType, left, out var leftRowPredicate))
            {
                var ids = FindMatchingIds<TRow>(tableResolver, navPlanRight);
                var nav = navPlanRight.Join.Kind switch
                {
                    Db2ReferenceNavigationKind.ForeignKeyToPrimaryKey => CompileForeignKeySemiJoin<TRow>(rootFile, navPlanRight.Join.RootKeyFieldSchema, ids),
                    Db2ReferenceNavigationKind.SharedPrimaryKeyOneToOne => CompileSharedPrimaryKeySemiJoin<TRow>(ids),
                    _ => _ => false,
                };

                rowPredicate = row => leftRowPredicate(row) && nav(row);
                return true;
            }

            if (Db2NavigationQueryTranslator.TryTranslateScalarPredicate(model, right, out var scalarPlanRight) &&
                Db2RowPredicateCompiler.TryCompile(rootFile, rootEntityType, left, out var leftRowPredicate2))
            {
                var ids = FindMatchingIdsScalar<TRow>(tableResolver, scalarPlanRight);
                var nav = scalarPlanRight.Join.Kind switch
                {
                    Db2ReferenceNavigationKind.ForeignKeyToPrimaryKey => CompileForeignKeySemiJoin<TRow>(rootFile, scalarPlanRight.Join.RootKeyFieldSchema, ids),
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
                Db2ReferenceNavigationKind.ForeignKeyToPrimaryKey => CompileForeignKeySemiJoin<TRow>(rootFile, scalarPlan.Join.RootKeyFieldSchema, scalarMatchingIds),
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
                Db2ReferenceNavigationKind.ForeignKeyToPrimaryKey => CompileNullCheckForeignKey<TRow>(rootFile, nullCheckPlan.Join.RootKeyFieldSchema, existingIds, nullCheckPlan.IsNotNull),
                Db2ReferenceNavigationKind.SharedPrimaryKeyOneToOne => row => existingIds.MatchesSharedPrimaryKeyNullCheck(Db2RowHandleAccess.AsHandle(row).RowId, nullCheckPlan.IsNotNull),
                _ => _ => false,
            };

            return true;
        }

        if (!Db2NavigationQueryTranslator.TryTranslateStringPredicate(model, predicate, out var plan))
            return false;

        var stringMatchingIds = FindMatchingIds<TRow>(tableResolver, plan);

        rowPredicate = plan.Join.Kind switch
        {
            Db2ReferenceNavigationKind.ForeignKeyToPrimaryKey => CompileForeignKeySemiJoin<TRow>(rootFile, plan.Join.RootKeyFieldSchema, stringMatchingIds),
            Db2ReferenceNavigationKind.SharedPrimaryKeyOneToOne => CompileSharedPrimaryKeySemiJoin<TRow>(stringMatchingIds),
            _ => _ => false,
        };

        return true;
    }

    private static Expression<Func<TEntity, bool>> RewriteCollectionCountComparisonsToAny<TEntity>(
        Db2Model model,
        Expression<Func<TEntity, bool>> predicate)
    {
        if (predicate.Parameters is not { Count: 1 })
            return predicate;

        var rewriter = new CollectionCountToAnyRewriter(model, predicate.Parameters[0]);
        var rewrittenBody = rewriter.Visit(predicate.Body);
        if (rewrittenBody is null || rewrittenBody == predicate.Body)
            return predicate;

        return Expression.Lambda<Func<TEntity, bool>>(rewrittenBody, predicate.Parameters[0]);
    }

    private sealed class CollectionCountToAnyRewriter(Db2Model model, ParameterExpression rootParam) : ExpressionVisitor
    {
        protected override Expression VisitBinary(BinaryExpression node)
        {
            var left = node.Left.UnwrapConvert();
            var right = node.Right.UnwrapConvert();

            if (TryRewrite(left, right, node.NodeType, out var rewritten) || TryRewrite(right, left, Flip(node.NodeType), out rewritten))
            {
                return rewritten;
            }

            return base.VisitBinary(node);

            bool TryRewrite(Expression countSide, Expression constantSide, ExpressionType op, out Expression result)
            {
                result = null!;

                if (!TryGetCollectionNavigationCountAccess(countSide, out var navMember, out var elementType, out var navAccess))
                    return false;

                if (constantSide is not ConstantExpression { Value: int constant })
                    return false;

                // count > 0  => Any()
                // count != 0 => Any()
                // count >= 1 => Any()
                var shouldRewriteToAny = op switch
                {
                    ExpressionType.GreaterThan => constant == 0,
                    ExpressionType.NotEqual => constant == 0,
                    ExpressionType.GreaterThanOrEqual => constant == 1,
                    _ => false,
                };

                if (!shouldRewriteToAny)
                    return false;

                if (!model.TryGetCollectionNavigation(rootParam.Type, navMember, out _))
                    return false;

                var enumerableType = typeof(IEnumerable<>).MakeGenericType(elementType);
                var source = navAccess.Type == enumerableType ? navAccess : Expression.Convert(navAccess, enumerableType);

                var anyMethod = typeof(Enumerable)
                    .GetMethods(BindingFlags.Public | BindingFlags.Static)
                    .Single(m => m.Name == nameof(Enumerable.Any)
                        && m.IsGenericMethodDefinition
                        && m.GetGenericArguments().Length == 1
                        && m.GetParameters().Length == 1)
                    .MakeGenericMethod(elementType);

                result = Expression.Call(anyMethod, source);
                return true;
            }

            bool TryGetCollectionNavigationCountAccess(
                Expression expression,
                out MemberInfo navMember,
                out Type elementType,
                out Expression navAccess)
            {
                navMember = null!;
                elementType = null!;
                navAccess = null!;

                if (expression is not MemberExpression { Member: PropertyInfo { Name: nameof(ICollection<>.Count) } } count)
                    return false;

                if (count.Expression.UnwrapConvert() is not MemberExpression { Member: PropertyInfo } nav)
                    return false;

                if (nav.Expression != rootParam)
                    return false;

                navMember = nav.Member;
                navAccess = nav;

                var navType = nav.Type;
                elementType = navType
                    .GetInterfaces()
                    .FirstOrDefault(i => i.IsGenericType && i.GetGenericTypeDefinition() == typeof(IEnumerable<>))
                    ?.GetGenericArguments()[0] ?? null!;

                return elementType is not null;
            }

            static ExpressionType Flip(ExpressionType op)
                => op switch
                {
                    ExpressionType.LessThan => ExpressionType.GreaterThan,
                    ExpressionType.LessThanOrEqual => ExpressionType.GreaterThanOrEqual,
                    ExpressionType.GreaterThan => ExpressionType.LessThan,
                    ExpressionType.GreaterThanOrEqual => ExpressionType.LessThanOrEqual,
                    _ => op,
                };
        }
    }

    private static HashSet<int> FindMatchingRootIdsForCollectionAny<TRow>(
        Db2Model model,
        Func<string, (IDb2File<TRow> File, Db2TableSchema Schema)> tableResolver,
        Db2CollectionNavigation navigation,
        LambdaExpression? dependentPredicate)
        where TRow : struct, IRowHandle
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
            : Expression.Lambda(
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
        where TRow : struct, IRowHandle
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
                var fk = Db2RowHandleAccess.ReadField<TRow, int>(dependentFile, row, fkIndex);
                if (fk != 0)
                    matchingRootIds.Add(fk);
            }

            return matchingRootIds;
        }

        var typedPredicate = (Expression<Func<TDependent, bool>>)dependentPredicate;

        if (Db2RowPredicateCompiler.TryCompile(dependentFile, dependentEntityType, typedPredicate, out var dependentRowPredicate))
        {
            foreach (var row in dependentFile.EnumerateRows())
            {
                if (!dependentRowPredicate(row))
                    continue;

                var fk = Db2RowHandleAccess.ReadField<TRow, int>(dependentFile, row, fkIndex);
                if (fk != 0)
                    matchingRootIds.Add(fk);
            }

            return matchingRootIds;
        }

        var dependentEntityPredicate = typedPredicate.Compile();
        var materializer = new Db2EntityMaterializer<TDependent, TRow>(dependentEntityType);

        foreach (var row in dependentFile.EnumerateRows())
        {
            var entity = materializer.Materialize(dependentFile, Db2RowHandleAccess.AsHandle(row));
            if (!dependentEntityPredicate(entity))
                continue;

            var fk = Db2RowHandleAccess.ReadField<TRow, int>(dependentFile, row, fkIndex);
            if (fk != 0)
                matchingRootIds.Add(fk);
        }

        return matchingRootIds;
    }

    private static HashSet<int> FindMatchingIds<TRow>(
        Func<string, (IDb2File<TRow> File, Db2TableSchema Schema)> tableResolver,
        Db2NavigationStringPredicatePlan plan)
        where TRow : struct, IRowHandle
    {
        var (relatedFile, _) = tableResolver(plan.Join.Target.TableName);

        var relatedFieldIndex = plan.TargetStringFieldSchema.ColumnStartIndex;
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

            var isDenseOptimizable = !relatedFile.Flags.HasFlag(Db2Flags.Sparse) && !plan.TargetStringFieldSchema.IsVirtual;
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

                    matchingIds.Add(Db2RowHandleAccess.AsHandle(relatedRow).RowId);
                }

                return matchingIds;
            }

            foreach (var relatedRow in relatedFile.EnumerateRows())
            {
                var s = Db2RowHandleAccess.ReadField<TRow, string>(relatedFile, relatedRow, relatedFieldIndex);

                var ok = plan.MatchKind switch
                {
                    Db2NavigationStringMatchKind.Contains => s.Contains(plan.Needle, StringComparison.Ordinal),
                    Db2NavigationStringMatchKind.StartsWith => s.StartsWith(plan.Needle, StringComparison.Ordinal),
                    Db2NavigationStringMatchKind.EndsWith => s.EndsWith(plan.Needle, StringComparison.Ordinal),
                    _ => false,
                };

                if (ok)
                    matchingIds.Add(Db2RowHandleAccess.AsHandle(relatedRow).RowId);
            }

            return matchingIds;
        }

        foreach (var relatedRow in relatedFile.EnumerateRows())
        {
            var s = Db2RowHandleAccess.ReadField<TRow, string>(relatedFile, relatedRow, relatedFieldIndex);
            if (s == plan.Needle)
                matchingIds.Add(Db2RowHandleAccess.AsHandle(relatedRow).RowId);
        }

        return matchingIds;
    }

    private static HashSet<int> FindMatchingIdsScalar<TRow>(
        Func<string, (IDb2File<TRow> File, Db2TableSchema Schema)> tableResolver,
        Db2NavigationScalarPredicatePlan plan)
        where TRow : struct, IRowHandle
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
        where TRow : struct, IRowHandle
        where T : unmanaged, IComparable<T>, IEquatable<T>
    {
        var (relatedFile, _) = tableResolver(plan.Join.Target.TableName);
        var relatedFieldIndex = plan.TargetScalarFieldSchema.ColumnStartIndex;
        HashSet<int> matchingIds = [];

        foreach (var relatedRow in relatedFile.EnumerateRows())
        {
            var value = Db2RowHandleAccess.ReadField<TRow, T>(relatedFile, relatedRow, relatedFieldIndex);
            if (EvaluateComparison(value, plan.ComparisonValue, plan.ComparisonKind))
                matchingIds.Add(Db2RowHandleAccess.AsHandle(relatedRow).RowId);
        }

        return matchingIds;
    }

    private static HashSet<int> FindExistingTargetIds<TRow>(
        Func<string, (IDb2File<TRow> File, Db2TableSchema Schema)> tableResolver,
        Db2NavigationNullCheckPlan plan)
        where TRow : struct, IRowHandle
    {
        var (relatedFile, _) = tableResolver(plan.Join.Target.TableName);
        HashSet<int> existingIds = [];

        foreach (var relatedRow in relatedFile.EnumerateRows())
        {
            var id = Db2RowHandleAccess.AsHandle(relatedRow).RowId;
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

    private static Func<TRow, bool> CompileNavigationSemiJoin<TRow>(IDb2File rootFile, Db2NavigationJoinPlan join, HashSet<int> matchingIds)
        where TRow : struct, IRowHandle
    {
        return join.Kind switch
        {
            Db2ReferenceNavigationKind.ForeignKeyToPrimaryKey => CompileForeignKeySemiJoin<TRow>(rootFile, join.RootKeyFieldSchema, matchingIds),
            Db2ReferenceNavigationKind.SharedPrimaryKeyOneToOne => CompileSharedPrimaryKeySemiJoin<TRow>(matchingIds),
            _ => _ => false,
        };
    }

    private static Func<TRow, bool> CompileNullCheckForeignKey<TRow>(IDb2File rootFile, Db2FieldSchema fkFieldSchema, HashSet<int> existingTargetIds, bool isNotNull)
        where TRow : struct, IRowHandle
    {
        if (fkFieldSchema.IsVirtual)
            throw new NotSupportedException($"Virtual foreign key field '{fkFieldSchema.Name}' is not supported for navigation predicates.");

        var rootFkIndex = fkFieldSchema.ColumnStartIndex;
        return row =>
        {
            var fk = Db2RowHandleAccess.ReadField<TRow, int>(rootFile, row, rootFkIndex);
            var exists = fk != 0 && existingTargetIds.Contains(fk);
            return isNotNull ? exists : !exists;
        };
    }

    private static Func<TRow, bool> CompileForeignKeySemiJoin<TRow>(IDb2File rootFile, Db2FieldSchema fkFieldSchema, HashSet<int> ids)
        where TRow : struct, IRowHandle
    {
        if (fkFieldSchema.IsVirtual)
            throw new NotSupportedException($"Virtual foreign key field '{fkFieldSchema.Name}' is not supported for navigation predicates.");

        var rootFkIndex = fkFieldSchema.ColumnStartIndex;
        return row =>
        {
            var fk = Db2RowHandleAccess.ReadField<TRow, int>(rootFile, row, rootFkIndex);
            return fk != 0 && ids.Contains(fk);
        };
    }

    private static Func<TRow, bool> CompileSharedPrimaryKeySemiJoin<TRow>(HashSet<int> matchingIds)
        where TRow : struct, IRowHandle
        => row =>
        {
            var id = Db2RowHandleAccess.AsHandle(row).RowId;
            return id != 0 && matchingIds.Contains(id);
        };
}
