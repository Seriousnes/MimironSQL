using System.Linq.Expressions;
using System.Linq;
using System.Reflection;

using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.Extensions.DependencyInjection;

using MimironSQL.EntityFrameworkCore.Db2.Model;
using MimironSQL.EntityFrameworkCore.Db2.Query.Expressions;
using MimironSQL.EntityFrameworkCore.Db2.Schema;
using MimironSQL.EntityFrameworkCore.Storage;
using MimironSQL.Formats;

namespace MimironSQL.EntityFrameworkCore.Db2.Query;

/// <summary>
/// Compiles <see cref="Db2QueryExpression"/> + shaper into executable code.
/// Produces expressions that create <see cref="Db2QueryingEnumerable{T}"/> at runtime.
/// </summary>
internal sealed class Db2ShapedQueryCompilingExpressionVisitor(
    ShapedQueryCompilingExpressionVisitorDependencies dependencies,
    QueryCompilationContext queryCompilationContext)
    : ShapedQueryCompilingExpressionVisitor(dependencies, queryCompilationContext)
{
    private static readonly MethodInfo CreateQueryingEnumerableMethod =
        typeof(Db2ShapedQueryCompilingExpressionVisitor)
            .GetMethod(nameof(CreateQueryingEnumerable), BindingFlags.NonPublic | BindingFlags.Static)!;

    private static readonly MethodInfo CreateProjectedQueryingEnumerableMethod =
        typeof(Db2ShapedQueryCompilingExpressionVisitor)
            .GetMethod(nameof(CreateProjectedQueryingEnumerable), BindingFlags.NonPublic | BindingFlags.Static)!;

    private static readonly MethodInfo CreateQueryingEnumerableWithIncludesMethod =
        typeof(Db2ShapedQueryCompilingExpressionVisitor)
            .GetMethod(nameof(CreateQueryingEnumerableWithIncludes), BindingFlags.NonPublic | BindingFlags.Static)!;

    private static readonly MethodInfo CreateJoinedQueryingEnumerableMethod =
        typeof(Db2ShapedQueryCompilingExpressionVisitor)
            .GetMethod(nameof(CreateJoinedQueryingEnumerable), BindingFlags.NonPublic | BindingFlags.Static)!;

    protected override Expression VisitExtension(Expression extensionExpression)
        => extensionExpression is Db2ScalarAggregateExpression
            ? extensionExpression
            : base.VisitExtension(extensionExpression);

    protected override Expression VisitShapedQuery(ShapedQueryExpression shapedQueryExpression)
    {
        // Handle scalar terminal operators (Any/Count/All) which are represented by a marker shaper expression.
        if (shapedQueryExpression.ShaperExpression is Db2ScalarAggregateExpression scalarAggregate)
        {
            if (shapedQueryExpression.QueryExpression is not Db2QueryExpression scalarDb2Query)
            {
                throw new NotSupportedException("Scalar aggregate queries are only supported for non-joined DB2 queries.");
            }

            scalarDb2Query.ApplyProjection();
            var scalarPlan = Db2QueryExecutionPlan.FromQueryExpression(scalarDb2Query);

            var scalarEntityType = scalarDb2Query.EntityType;
            var scalarEntityClrType = scalarEntityType.ClrType;

            var scalarQueryContextParam = QueryCompilationContext.QueryContextParameter;

            // Build the base enumerable (without include post-processing) and apply the scalar operator.
            var createEnumerable = Expression.Call(
                CreateQueryingEnumerableMethod.MakeGenericMethod(scalarEntityClrType),
                scalarQueryContextParam,
                Expression.Constant(scalarPlan),
                Expression.Constant(scalarEntityType));

            Expression scalar = scalarAggregate.Kind switch
            {
                Db2ScalarAggregateKind.Any => Expression.Call(
                    typeof(Enumerable),
                    nameof(Enumerable.Any),
                    [scalarEntityClrType],
                    createEnumerable),

                Db2ScalarAggregateKind.Count => Expression.Call(
                    typeof(Enumerable),
                    nameof(Enumerable.Count),
                    [scalarEntityClrType],
                    createEnumerable),

                _ => throw new NotSupportedException($"Unsupported scalar aggregate kind: {scalarAggregate.Kind}"),
            };

            if (scalarAggregate is { Kind: Db2ScalarAggregateKind.Any, IsNegated: true })
            {
                scalar = Expression.Not(scalar);
            }

            // EF Core's compilation pipeline expects query compilation to yield a sequence.
            // Wrap the scalar in a single-element IEnumerable so EF can apply ResultCardinality processing.
            return Expression.Call(
                typeof(Enumerable),
                nameof(Enumerable.Repeat),
                [scalar.Type],
                scalar,
                Expression.Constant(1));
        }

        // Handle joined query expressions
        if (shapedQueryExpression.QueryExpression is Db2JoinedQueryExpression joinedQuery)
        {
            return VisitJoinedShapedQuery(shapedQueryExpression, joinedQuery);
        }

        var db2Query = (Db2QueryExpression)shapedQueryExpression.QueryExpression;

        // Finalize projections
        db2Query.ApplyProjection();

        // Create execution plan
        var plan = Db2QueryExecutionPlan.FromQueryExpression(db2Query);

        // Build property-to-field-index mapping for the entity type
        var entityType = db2Query.EntityType;
        var entityClrType = entityType.ClrType;

        var shaperExpression = shapedQueryExpression.ShaperExpression;
        var resultType = shaperExpression.Type;

        var queryContextParam = QueryCompilationContext.QueryContextParameter;
        var isAsync = QueryCompilationContext.IsAsync;

        // Retrieve stored includes (if any were extracted during preprocessing)
        var storedIncludes = Db2IncludeStorage.RetrieveAndClear();

        // For simple entity queries (shaper is StructuralTypeShaperExpression), return directly
        if (shaperExpression is StructuralTypeShaperExpression)
        {
            Expression createEnumerable;

            if (storedIncludes is { IncludeChains.Count: > 0 })
            {
                // Create enumerable with includes applied
                createEnumerable = Expression.Call(
                    CreateQueryingEnumerableWithIncludesMethod.MakeGenericMethod(entityClrType),
                    queryContextParam,
                    Expression.Constant(plan),
                    Expression.Constant(entityType),
                    Expression.Constant(storedIncludes.IncludeChains),
                    Expression.Constant(storedIncludes.IgnoreAutoIncludes));
            }
            else
            {
                createEnumerable = Expression.Call(
                    CreateQueryingEnumerableMethod.MakeGenericMethod(entityClrType),
                    queryContextParam,
                    Expression.Constant(plan),
                    Expression.Constant(entityType));
            }

            if (isAsync)
            {
                createEnumerable = Expression.Convert(
                    createEnumerable,
                    typeof(IAsyncEnumerable<>).MakeGenericType(entityClrType));
            }

            return createEnumerable;
        }

        // For projected queries, check for navigation property access which is not supported
        if (NavigationDetector.ContainsNavigationAccess(shaperExpression, entityType))
        {
            throw new NotSupportedException("Projecting navigation properties is not supported by the Db2 provider.");
        }

        // For projected queries, compose the projection into the shaper to produce
        // Db2QueryingEnumerable<TResult>, which implements both IEnumerable<T> and IAsyncEnumerable<T>.
        var entityParam = Expression.Parameter(entityClrType, "entity");
        var projectionBody = new StructuralTypeShaperReplacer(entityParam).Visit(shaperExpression);
        var projectionLambda = Expression.Lambda(
            typeof(Func<,>).MakeGenericType(entityClrType, resultType),
            projectionBody,
            entityParam);
        var compiledProjection = projectionLambda.Compile();

        Expression createProjectedEnumerable = Expression.Call(
            CreateProjectedQueryingEnumerableMethod.MakeGenericMethod(entityClrType, resultType),
            queryContextParam,
            Expression.Constant(plan),
            Expression.Constant(entityType),
            Expression.Constant(compiledProjection, typeof(Delegate)));

        if (isAsync)
        {
            createProjectedEnumerable = Expression.Convert(
                createProjectedEnumerable,
                typeof(IAsyncEnumerable<>).MakeGenericType(resultType));
        }

        return createProjectedEnumerable;
    }

    /// <summary>
    /// Visits a shaped query with a joined query expression.
    /// Creates a runtime enumerable that executes the joined query.
    /// </summary>
    private Expression VisitJoinedShapedQuery(ShapedQueryExpression shapedQueryExpression, Db2JoinedQueryExpression joinedQuery)
    {
        var shaperExpression = shapedQueryExpression.ShaperExpression;
        
        var outerQuery = joinedQuery.Outer;

        // Create execution plan for the outer query
        outerQuery.ApplyProjection();
        var outerPlan = Db2QueryExecutionPlan.FromQueryExpression(outerQuery);

        // Build joined execution plan
        var joinedPlan = Db2JoinedQueryExecutionPlan.FromJoinedQueryExpression(joinedQuery, outerPlan);

        var outerEntityType = outerQuery.EntityType;
        var outerClrType = outerEntityType.ClrType;
        var innerEntityType = joinedQuery.InnerEntityType;
        var innerClrType = innerEntityType.ClrType;

        var queryContextParam = QueryCompilationContext.QueryContextParameter;
        var isAsync = QueryCompilationContext.IsAsync;

        var resultType = shaperExpression.Type;

        // Compile the result projection from the shaper expression
        // The shaper may access m.Inner/m.Outer which need to be replaced with the actual entities
        var resultProjection = CompileJoinedResultProjection(shaperExpression, outerClrType, innerClrType, resultType);

        // Create the joined enumerable
        Expression createEnumerable = Expression.Call(
            CreateJoinedQueryingEnumerableMethod.MakeGenericMethod(outerClrType, innerClrType, resultType),
            queryContextParam,
            Expression.Constant(joinedPlan),
            Expression.Constant(outerEntityType),
            Expression.Constant(innerEntityType),
            Expression.Constant(resultProjection));

        if (isAsync)
        {
            createEnumerable = Expression.Convert(
                createEnumerable,
                typeof(IAsyncEnumerable<>).MakeGenericType(resultType));
        }

        return createEnumerable;
    }

    /// <summary>
    /// Compiles a result projection delegate from the shaper expression.
    /// Replaces references to Inner/Outer with actual entity parameters.
    /// </summary>
    private static Delegate CompileJoinedResultProjection(Expression shaperExpression, Type outerClrType, Type innerClrType, Type resultType)
    {
        // Create parameters for the projection delegate: (outer, inner) => result
        var outerParam = Expression.Parameter(outerClrType, "outer");
        var innerParam = Expression.Parameter(innerClrType, "inner");

        // Replace references to Inner/Outer in the shaper expression with our parameters
        var replacer = new JoinedShaperReplacer(outerParam, innerParam, outerClrType, innerClrType);
        var projectionBody = replacer.Visit(shaperExpression);

        // Create and compile the lambda
        var delegateType = typeof(Func<,,>).MakeGenericType(outerClrType, innerClrType, resultType);
        var lambda = Expression.Lambda(delegateType, projectionBody, outerParam, innerParam);
        return lambda.Compile();
    }

    /// <summary>
    /// Replaces references to Inner/Outer in joined shaper expressions with actual entity parameters.
    /// </summary>
    private sealed class JoinedShaperReplacer(
        ParameterExpression outerParam,
        ParameterExpression innerParam,
        Type outerType,
        Type innerType) : ExpressionVisitor
    {
        protected override Expression VisitMember(MemberExpression node)
        {
            // Handle field access patterns like m.Inner or m.Outer from TransparentIdentifier
            // Where m is the lambda parameter
            if (node.Member.Name == "Inner" && node.Expression is ParameterExpression)
            {
                return innerParam;
            }
            if (node.Member.Name == "Outer" && node.Expression is ParameterExpression)
            {
                return outerParam;
            }

            // Handle Db2JoinedShaperExpression.Inner -> innerParam
            // Handle Db2JoinedShaperExpression.Outer -> outerParam
            if (node.Expression is Db2JoinedShaperExpression)
            {
                if (node.Member.Name == "Inner")
                    return innerParam;
                if (node.Member.Name == "Outer")
                    return outerParam;
            }

            // Handle nested patterns like m.Inner.Property or Db2JoinedShaperExpression.Inner.Property
            if (node.Expression is MemberExpression parent)
            {
                var visitedParent = Visit(parent);
                if (visitedParent != parent)
                {
                    return Expression.MakeMemberAccess(visitedParent, node.Member);
                }
            }

            return base.VisitMember(node);
        }

        protected override Expression VisitExtension(Expression node)
        {
            // Handle StructuralTypeShaperExpression for outer/inner entities
            if (node is StructuralTypeShaperExpression shaper)
            {
                if (shaper.StructuralType is IEntityType entityType)
                {
                    if (entityType.ClrType == outerType)
                        return outerParam;
                    if (entityType.ClrType == innerType)
                        return innerParam;
                }
            }

            // Handle Db2JoinedShaperExpression - identity case
            // When accessed alone (not .Inner/.Outer), this shouldn't happen in our usage
            if (node is Db2JoinedShaperExpression)
            {
                // This case means someone wants the whole joined result, not just inner/outer
                // For now, return the outer entity as a fallback
                return outerParam;
            }

            return base.VisitExtension(node);
        }
    }

    /// <summary>
    /// Replaces <see cref="StructuralTypeShaperExpression"/> nodes in the expression tree
    /// with a parameter expression representing the materialized entity.
    /// </summary>
    private sealed class StructuralTypeShaperReplacer(ParameterExpression replacement) : ExpressionVisitor
    {
        protected override Expression VisitExtension(Expression node)
            => node is StructuralTypeShaperExpression ? replacement : base.VisitExtension(node);
    }

    /// <summary>
    /// Detects whether an expression tree contains access to navigation properties,
    /// which are not supported by the Db2 provider.
    /// </summary>
    private sealed class NavigationDetector(IEntityType entityType) : ExpressionVisitor
    {
        private bool _foundNavigation;

        public static bool ContainsNavigationAccess(Expression expression, IEntityType entityType)
        {
            var detector = new NavigationDetector(entityType);
            detector.Visit(expression);
            return detector._foundNavigation;
        }

        protected override Expression VisitMember(MemberExpression node)
        {
            // Check if the member access is on a StructuralTypeShaperExpression (entity access)
            // and if that member is a navigation property
            if (node.Expression is StructuralTypeShaperExpression && node.Member is System.Reflection.PropertyInfo property)
            {
                var navigation = entityType.FindNavigation(property.Name);
                if (navigation is not null)
                {
                    _foundNavigation = true;
                    return node;
                }
            }

            return base.VisitMember(node);
        }
    }

    /// <summary>
    /// Creates a <see cref="Db2QueryingEnumerable{T}"/> at runtime.
    /// This method is called from the compiled query expression.
    /// </summary>
    private static IEnumerable<T> CreateQueryingEnumerable<T>(
        QueryContext queryContext,
        Db2QueryExecutionPlan plan,
        IEntityType entityType)
    {
        var db2Context = (Db2QueryContext)queryContext;
        var (file, schema) = db2Context.Store.OpenTableWithSchema(plan.TableName);

        // Create entity factory for lazy loading proxy support
        var entityFactory = CreateEntityFactory(db2Context);

        // Build the shaper that materializes entities from row handles
        var shaper = BuildShaper<T>(entityType, schema, entityFactory);

        return new Db2QueryingEnumerable<T>(queryContext, plan, shaper);
    }

    /// <summary>
    /// Creates a <see cref="Db2QueryingEnumerable{T}"/> at runtime with Include support.
    /// Applies batched include loading after materializing the root entities.
    /// </summary>
    private static IEnumerable<T> CreateQueryingEnumerableWithIncludes<T>(
        QueryContext queryContext,
        Db2QueryExecutionPlan plan,
        IEntityType entityType,
        IReadOnlyList<MemberInfo[]> includeChains,
        bool ignoreAutoIncludes)
        where T : class
    {
        var db2Context = (Db2QueryContext)queryContext;
        var (file, schema) = db2Context.Store.OpenTableWithSchema(plan.TableName);

        // Create entity factory for lazy loading proxy support
        var entityFactory = CreateEntityFactory(db2Context);

        // Build the shaper that materializes entities from row handles
        var shaper = BuildShaper<T>(entityType, schema, entityFactory);

        // Materialize the base enumerable
        var baseEnumerable = new Db2QueryingEnumerable<T>(queryContext, plan, shaper);

        // Apply includes using the batched executor
        return ApplyIncludes<T>(baseEnumerable, db2Context, includeChains, ignoreAutoIncludes);
    }

    /// <summary>
    /// Creates an entity factory that supports lazy loading proxies.
    /// </summary>
    private static IDb2EntityFactory CreateEntityFactory(Db2QueryContext db2Context)
        => new EfLazyLoadingProxyDb2EntityFactory(db2Context.Context, new ReflectionDb2EntityFactory());

    /// <summary>
    /// Applies include chains to a materialized enumerable using batched loading.
    /// </summary>
    private static IEnumerable<T> ApplyIncludes<T>(
        IEnumerable<T> source,
        Db2QueryContext db2Context,
        IReadOnlyList<MemberInfo[]> includeChains,
        bool ignoreAutoIncludes)
        where T : class
    {
        // Get the model binding for navigation resolution
        var modelBinding = db2Context.Context.GetService<IDb2ModelBinding>()?.GetBinding();
        if (modelBinding is null)
        {
            return source;
        }

        // Create an entity factory for related entities
        var entityFactory = CreateEntityFactory(db2Context);

        // Table resolver function for opening related tables
        (IDb2File<RowHandle> File, Db2TableSchema Schema) TableResolver(string tableName)
            => db2Context.Store.OpenTableWithSchema<RowHandle>(tableName);

        // Apply each include chain
        IEnumerable<T> current = source;
        for (var i = 0; i < includeChains.Count; i++)
        {
            current = Db2IncludeChainExecutor.Apply<T, RowHandle>(
                current,
                modelBinding,
                TableResolver,
                includeChains[i],
                entityFactory);
        }

        return current;
    }

    /// <summary>
    /// Creates a joined query enumerable that combines outer and inner entities.
    /// Executes a join between two DB2 tables at runtime with filtering support.
    /// </summary>
    private static IEnumerable<TResult> CreateJoinedQueryingEnumerable<TOuter, TInner, TResult>(
        QueryContext queryContext,
        Db2JoinedQueryExecutionPlan plan,
        IEntityType outerEntityType,
        IEntityType innerEntityType,
        Delegate resultProjection)
        where TOuter : class
        where TInner : class
    {
        var db2Context = (Db2QueryContext)queryContext;

        // Open both tables
        var (outerFile, outerSchema) = db2Context.Store.OpenTableWithSchema(plan.OuterPlan.TableName);
        var (innerFile, innerSchema) = db2Context.Store.OpenTableWithSchema(plan.InnerTableName);

        // Create entity factory for lazy loading proxy support
        var entityFactory = CreateEntityFactory(db2Context);

        // Build shapers for both entity types
        var outerShaper = BuildShaper<TOuter>(outerEntityType, outerSchema, entityFactory);
        var innerShaper = BuildShaper<TInner>(innerEntityType, innerSchema, entityFactory);

        // Get the key column indexes
        if (!outerSchema.TryGetFieldCaseInsensitive(plan.OuterKeyColumn, out var outerKeyField))
        {
            throw new InvalidOperationException($"Outer key column '{plan.OuterKeyColumn}' not found in table '{plan.OuterPlan.TableName}'.");
        }
        if (!innerSchema.TryGetFieldCaseInsensitive(plan.InnerKeyColumn, out var innerKeyField))
        {
            throw new InvalidOperationException($"Inner key column '{plan.InnerKeyColumn}' not found in table '{plan.InnerTableName}'.");
        }

        var outerKeyIndex = outerKeyField.ColumnStartIndex;
        var innerKeyIndex = innerKeyField.ColumnStartIndex;

        // Build a lookup of inner entities by their key
        var innerLookup = new Dictionary<int, (TInner Entity, RowHandle Handle)>();
        foreach (var handle in innerFile.EnumerateRowHandles())
        {
            var keyValue = innerFile.ReadField<int>(handle, innerKeyIndex);
            if (keyValue != 0 && !innerLookup.ContainsKey(keyValue))
            {
                var entity = innerShaper(queryContext, innerFile, handle);
                innerLookup[keyValue] = (entity, handle);
            }
        }

        // Compile the joined filter if present
        Func<IDb2File, RowHandle, IDb2File, RowHandle?, bool>? joinedFilter = null;
        if (plan.JoinedFilter is not null)
        {
            joinedFilter = Db2JoinedQueryingEnumerable.CompileJoinedFilter(
                plan.JoinedFilter,
                outerSchema,
                innerSchema,
                queryContext,
                db2Context.Store);
        }

        var typedProjection = (Func<TOuter, TInner?, TResult>)resultProjection;

        // Enumerate the outer table and join with inner
        return new Db2JoinedQueryingEnumerable<TOuter, TInner, TResult>(
            queryContext,
            plan,
            outerFile,
            outerSchema,
            innerFile,
            innerSchema,
            outerShaper,
            innerShaper,
            innerLookup,
            outerKeyIndex,
            joinedFilter,
            typedProjection);
    }

    /// <summary>
    /// Creates a projected <see cref="Db2QueryingEnumerable{TResult}"/> at runtime.
    /// Composes entity materialization with a projection to produce the result type directly,
    /// ensuring the enumerable supports both sync and async enumeration.
    /// </summary>
    private static IEnumerable<TResult> CreateProjectedQueryingEnumerable<TEntity, TResult>(
        QueryContext queryContext,
        Db2QueryExecutionPlan plan,
        IEntityType entityType,
        Delegate projection)
    {
        var db2Context = (Db2QueryContext)queryContext;
        var (file, schema) = db2Context.Store.OpenTableWithSchema(plan.TableName);

        // Create entity factory for lazy loading proxy support
        var entityFactory = CreateEntityFactory(db2Context);

        var entityShaper = BuildShaper<TEntity>(entityType, schema, entityFactory);
        var typedProjection = (Func<TEntity, TResult>)projection;

        return new Db2QueryingEnumerable<TResult>(
            queryContext,
            plan,
            (qc, f, h) => typedProjection(entityShaper(qc, f, h)));
    }

    /// <summary>
    /// Builds a shaper function that reads entity properties from DB2 fields.
    /// Skips navigation properties and collection types that require special handling.
    /// </summary>
    /// <param name="entityType">The EF entity type metadata.</param>
    /// <param name="schema">The DB2 table schema.</param>
    /// <param name="entityFactory">Factory for creating entity instances (supports lazy loading proxies).</param>
    private static Func<QueryContext, IDb2File, RowHandle, T> BuildShaper<T>(
        IEntityType entityType,
        Db2TableSchema schema,
        IDb2EntityFactory entityFactory)
    {
        var clrType = entityType.ClrType;
        var properties = entityType.GetProperties().ToArray();

        // Build property-to-field-index map for scalar properties only
        var propertyFieldMap = new List<(PropertyInfo Property, int FieldIndex, Type ClrType, bool IsArray)>();

        for (var i = 0; i < properties.Length; i++)
        {
            var property = properties[i];
            var propertyInfo = property.PropertyInfo;
            if (propertyInfo is null)
                continue;

            var propType = propertyInfo.PropertyType;

            // Skip navigation properties (they're handled by Include processing)
            if (entityType.FindNavigation(property.Name) is not null)
                continue;

            var columnName = property.GetColumnName() ?? property.Name;
            if (!schema.TryGetFieldCaseInsensitive(columnName, out var fieldSchema))
                continue;

            // Handle array properties (T[] backed by schema arrays)
            if (propType.IsArray)
            {
                var elementType = propType.GetElementType()!;
                if (elementType != typeof(string) && fieldSchema.ElementCount > 1)
                {
                    // Store as array type for ReadField<T[]>
                    propertyFieldMap.Add((propertyInfo, fieldSchema.ColumnStartIndex, propType, true));
                }
                continue;
            }

            // Handle ICollection<T> backed by schema arrays
            if (propType.IsGenericType && fieldSchema.ElementCount > 1)
            {
                var genericDef = propType.GetGenericTypeDefinition();
                if (genericDef == typeof(ICollection<>) || genericDef == typeof(IList<>) || genericDef == typeof(List<>))
                {
                    var elementType = propType.GetGenericArguments()[0];
                    if (elementType != typeof(string) && (elementType.IsPrimitive || elementType == typeof(float) || elementType == typeof(double)))
                    {
                        // Store array type for ReadField, then convert to collection
                        propertyFieldMap.Add((propertyInfo, fieldSchema.ColumnStartIndex, elementType.MakeArrayType(), true));
                    }
                    continue;
                }
            }

            // Skip other collection types that we don't handle
            if (typeof(System.Collections.IEnumerable).IsAssignableFrom(propType) && propType != typeof(string))
                continue;

            // Scalar property
            propertyFieldMap.Add((propertyInfo, fieldSchema.ColumnStartIndex, propType, false));
        }

        var propertyFieldMapArray = propertyFieldMap.ToArray();

        return (queryContext, db2File, handle) =>
        {
            // Use entity factory to create instance (supports lazy loading proxies)
            var entity = entityFactory.Create(clrType);

            for (var i = 0; i < propertyFieldMapArray.Length; i++)
            {
                var (prop, fieldIndex, readType, isArray) = propertyFieldMapArray[i];
                if (prop is null)
                    continue;

                object? value;
                if (isArray)
                {
                    // ReadField knows how to read arrays with the array type
                    value = ReadArrayField(db2File, handle, fieldIndex, readType, prop.PropertyType);
                }
                else
                {
                    value = ReadFieldValue(db2File, handle, fieldIndex, readType);
                }
                prop.SetValue(entity, value);
            }

            return (T)entity;
        };
    }

    /// <summary>
    /// Reads an array field from the DB2 file using the file's native array reading.
    /// </summary>
    private static object? ReadArrayField(IDb2File file, RowHandle handle, int fieldIndex, Type arrayType, Type targetPropertyType)
    {
        // Use the file's ReadField method which handles arrays natively
        var value = ReadPrimitiveField(file, handle, fieldIndex, arrayType);
        
        if (value is null)
            return null;

        // If the target property is ICollection<T>, return as-is since array implements ICollection<T>
        if (targetPropertyType.IsAssignableFrom(arrayType))
            return value;

        // For ICollection<T> properties, the array can be assigned directly
        return value;
    }

    private static object? ReadFieldValue(IDb2File file, RowHandle handle, int fieldIndex, Type clrType)
    {
        // Handle nullable types by reading the underlying type
        var underlyingNullable = Nullable.GetUnderlyingType(clrType);
        if (underlyingNullable is not null)
            clrType = underlyingNullable;

        // Handle enum types by reading the underlying integral type and converting
        if (clrType.IsEnum)
        {
            var enumUnderlying = Enum.GetUnderlyingType(clrType);
            var rawValue = ReadPrimitiveField(file, handle, fieldIndex, enumUnderlying);
            return rawValue is not null ? Enum.ToObject(clrType, rawValue) : null;
        }

        return ReadPrimitiveField(file, handle, fieldIndex, clrType);
    }

    private static object? ReadPrimitiveField(IDb2File file, RowHandle handle, int fieldIndex, Type clrType)
    {
        // Scalar types
        if (clrType == typeof(int)) return file.ReadField<int>(handle, fieldIndex);
        if (clrType == typeof(uint)) return file.ReadField<uint>(handle, fieldIndex);
        if (clrType == typeof(long)) return file.ReadField<long>(handle, fieldIndex);
        if (clrType == typeof(ulong)) return file.ReadField<ulong>(handle, fieldIndex);
        if (clrType == typeof(short)) return file.ReadField<short>(handle, fieldIndex);
        if (clrType == typeof(ushort)) return file.ReadField<ushort>(handle, fieldIndex);
        if (clrType == typeof(byte)) return file.ReadField<byte>(handle, fieldIndex);
        if (clrType == typeof(sbyte)) return file.ReadField<sbyte>(handle, fieldIndex);
        if (clrType == typeof(float)) return file.ReadField<float>(handle, fieldIndex);
        if (clrType == typeof(double)) return file.ReadField<double>(handle, fieldIndex);
        if (clrType == typeof(bool)) return file.ReadField<bool>(handle, fieldIndex);
        if (clrType == typeof(string)) return file.ReadField<string>(handle, fieldIndex);

        // Array types
        if (clrType == typeof(int[])) return file.ReadField<int[]>(handle, fieldIndex);
        if (clrType == typeof(uint[])) return file.ReadField<uint[]>(handle, fieldIndex);
        if (clrType == typeof(long[])) return file.ReadField<long[]>(handle, fieldIndex);
        if (clrType == typeof(ulong[])) return file.ReadField<ulong[]>(handle, fieldIndex);
        if (clrType == typeof(short[])) return file.ReadField<short[]>(handle, fieldIndex);
        if (clrType == typeof(ushort[])) return file.ReadField<ushort[]>(handle, fieldIndex);
        if (clrType == typeof(byte[])) return file.ReadField<byte[]>(handle, fieldIndex);
        if (clrType == typeof(sbyte[])) return file.ReadField<sbyte[]>(handle, fieldIndex);
        if (clrType == typeof(float[])) return file.ReadField<float[]>(handle, fieldIndex);
        if (clrType == typeof(double[])) return file.ReadField<double[]>(handle, fieldIndex);

        throw new NotSupportedException($"Unsupported field CLR type for ReadField: {clrType.FullName}");
    }
}
