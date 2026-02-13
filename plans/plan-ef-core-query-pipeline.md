# Plan: Adopt EF Core's Standard Query Pipeline

## Status: Draft
## Date: 2026-02-13

---

## 1. Problem Statement

The current `MimironSQL.EntityFrameworkCore` provider **bypasses EF Core's standard query pipeline entirely**. Instead of using the standard `QueryCompilationContext` → `QueryableMethodTranslatingExpressionVisitor` → `ShapedQueryCompilingExpressionVisitor` pipeline, it:

1. **Replaces `IQueryCompiler`** with `MimironDb2QueryExecutor`, which manually walks the LINQ expression tree.
2. **Strips EF-specific expressions** (Include, AsNoTracking, etc.) via `Db2ExpressionPreprocessor`.
3. **Materializes entities in-memory**, then hands remaining LINQ operators to LINQ-to-Objects via `CompileAndExecute`.

This causes practical problems:
- `EF.Property<T>()` calls fail at runtime because LINQ-to-Objects doesn't understand them — requiring `RewriteEfPropertyCalls`.
- EF Core's built-in optimizations (compiled query cache, navigation expansion, etc.) are unavailable.
- The expression tree analysis is fragile; every new LINQ pattern requires manual handling in `AnalyzePreEntityChain`, `TryGetKeyLookupInfo`, etc.
- Include/navigation handling is reimplemented from scratch rather than leveraging EF Core's `NavigationExpandingExpressionVisitor`.

**Goal**: Adopt EF Core's standard query compilation pipeline so the provider participates in the same expression tree flow as Cosmos, InMemory, and relational providers. Instead of translating to SQL, the pipeline should translate LINQ operators into a `Db2QueryExpression` that represents optimized DB2 file reads — and compile that expression into executable code that calls `IDb2File.ReadField<T>`, `EnumerateRows`, `TryGetRowById`, etc.

---

## 2. Architecture Overview

### 2.1 Standard EF Core Query Pipeline

```
User LINQ query (Expression)
    │
    ▼
QueryCompiler.Execute<TResult>(Expression)
    │
    ▼
IDatabase.CompileQuery<TResult>(query, async)
    │
    ▼
QueryCompilationContext.CreateQueryExecutor<TResult>(query)
    │
    ├── 1. Preprocessing:    QueryTranslationPreprocessor.Process(query)
    │       ► NavigationExpandingExpressionVisitor
    │       ► Converts Include() calls into NavigationExpansionExpression nodes
    │
    ├── 2. Translation:      QueryableMethodTranslatingExpressionVisitor.Translate(preprocessed)
    │       ► Visits each Queryable method call (Where, Select, OrderBy, etc.)
    │       ► Produces ShapedQueryExpression { QueryExpression, ShaperExpression }
    │       ► QueryExpression is the provider-specific representation
    │
    ├── 3. Postprocessing:   QueryTranslationPostprocessor.Process(translated)
    │       ► Validates all nodes were translated
    │
    ├── 4. Compilation:      ShapedQueryCompilingExpressionVisitor.Visit(shaped)
    │       ► Converts ShapedQueryExpression into executable Func<QueryContext, TResult>
    │       ► Injects entity materializers from EF Core
    │
    └── 5. Runtime params:   InsertRuntimeParameters(compiled)
    │
    ▼
Func<QueryContext, TResult> — cached and executed
```

### 2.2 Current MimironSQL Pipeline (to be replaced)

```
MimironDb2QueryExecutor.Execute<TResult>(Expression)
    │
    ├── Normalize (MimironDb2EfExpressionNormalizer)
    ├── Determine entity type + route to ExecuteTyped<TEntity, TRow, TResult>
    │
    ├── Key-Lookup Path:
    │   ├── Detect PK-based Where (TryGetKeyLookupInfo)
    │   ├── Materialize by IDs directly
    │   ├── Apply Include chains manually
    │   └── Compile residual → LINQ-to-Objects (CompileAndExecute)   ← EF.Property<T> problem
    │
    └── Provider Path:
        ├── Create Db2QueryProvider + Db2Queryable
        ├── Rewrite expression root
        └── Db2QueryProvider.Execute<TResult>(...)
            ├── AnalyzePreEntityChain (manual expression walk)
            ├── Try pruned projection (Db2RowProjectorCompiler)
            ├── EnumerateEntities with row-level predicates (Db2RowPredicateCompiler)
            ├── Apply Include chains manually (Db2IncludeChainExecutor)
            └── Build residual → LINQ-to-Objects (CompileAndExecute)  ← EF.Property<T> problem
```

### 2.3 Proposed MimironSQL Pipeline

```
Standard EF Core QueryCompiler.Execute<TResult>(Expression)        [no override needed]
    │
    ▼
MimironDb2Database.CompileQuery<TResult>(query, async)             [updated]
    │
    ▼
QueryCompilationContext.CreateQueryExecutor<TResult>(query)        [base class, no override]
    │
    ├── 1. Db2QueryTranslationPreprocessor.Process(query)
    │       ► default NavigationExpandingExpressionVisitor (handles Includes natively)
    │
    ├── 2. Db2QueryableMethodTranslatingExpressionVisitor.Translate(preprocessed)
    │       ► CreateShapedQueryExpression → creates Db2QueryExpression
    │       ► TranslateWhere → compiles predicate → Db2QueryExpression.ApplyFilter(...)
    │       ► TranslateSelect → adjusts projection
    │       ► TranslateFirstOrDefault → sets cardinality, applies Take(1)
    │       ► etc.
    │       ► Returns ShapedQueryExpression { Db2QueryExpression, ShaperExpression }
    │
    ├── 3. QueryTranslationPostprocessor.Process(translated)       [base class]
    │
    ├── 4. Db2ShapedQueryCompilingExpressionVisitor.VisitShapedQuery(shaped)
    │       ► Reads Db2QueryExpression
    │       ► Emits code that:
    │         - Opens IDb2File from IMimironDb2Store
    │         - Applies PK-based lookups / full scans based on filter analysis
    │         - Projects fields directly via ReadField<T>
    │         - Materializes entities using EF Core's standard materializer injection
    │       ► Returns Expression<Func<QueryContext, TResult>>
    │
    └── 5. InsertRuntimeParameters
    │
    ▼
Func<QueryContext, TResult> — cached and executed
```

---

## 3. New Types to Create

### 3.1 `Db2QueryExpression` — Server-Side Query Representation

**Location**: `src/MimironSQL.EntityFrameworkCore/Db2/Query/Expressions/Db2QueryExpression.cs`

This is the central type — analogous to Cosmos' `SelectExpression` or InMemory's `InMemoryQueryExpression`. It represents a DB2 table scan with optional filters, projections, ordering, and limits.

```csharp
internal sealed class Db2QueryExpression : Expression, IPrintableExpression
{
    // ── Source ──
    public IEntityType EntityType { get; }          // The EF entity type being queried
    public string TableName { get; }                 // The DB2 table name
    
    // ── Projection ──
    // Maps ProjectionMembers → Db2 field expressions.
    // Initially maps all entity properties to their field indices.
    private Dictionary<ProjectionMember, Expression> _projectionMapping;
    private List<Db2ProjectionExpression> _projection;  // finalized after ApplyProjection()
    
    // ── Filter ──
    // A tree of Db2 filter expressions, built up by TranslateWhere.
    public Db2FilterExpression? Filter { get; private set; }
    
    // ── Pagination ──
    public int? Limit { get; private set; }
    public int? Offset { get; private set; }
    
    // ── Ordering ──
    public IReadOnlyList<Db2OrderingExpression> Orderings { get; }
    
    // ── Mutation methods ──
    public void ApplyFilter(Db2FilterExpression filter);   // AND-combines filters
    public void ApplyProjection();                          // Finalizes projection mapping
    public void ApplyLimit(int limit);
    public void ApplyOffset(int offset);
    public void ApplyOrdering(Db2OrderingExpression ordering);
    
    // Expression overrides
    public override Type Type => typeof(IEnumerable<ValueBuffer>);
    public override ExpressionType NodeType => ExpressionType.Extension;
}
```

**Design rationale**: Following InMemory's pattern, `Type` returns `IEnumerable<ValueBuffer>`. The `ValueBuffer` is EF Core's row abstraction — each entity property is read by index from it. During compilation (`VisitShapedQuery`), the `Db2QueryExpression` is lowered into code that populates `ValueBuffer` instances from `IDb2File.ReadField<T>` calls.

### 3.2 Db2 Expression Node Types

**Location**: `src/MimironSQL.EntityFrameworkCore/Db2/Query/Expressions/`

These represent filter/projection operations in the DB2 query expression tree. Unlike SQL, DB2 files support very limited operations, so the expression tree is intentionally sparse.

```
Db2FilterExpression (abstract)
├── Db2ComparisonFilterExpression       # field == value, field > value, etc.
│       Properties: Db2FieldAccess Field, ComparisonKind, object? Value
├── Db2ContainsFilterExpression         # field IN (1, 2, 3) / ids.Contains(field)
│       Properties: Db2FieldAccess Field, IReadOnlyList<object> Values
├── Db2StringMatchFilterExpression      # string Contains/StartsWith/EndsWith (uses dense string table)
│       Properties: Db2FieldAccess Field, StringMatchKind, string Pattern
├── Db2AndFilterExpression              # left AND right
│       Properties: Db2FilterExpression Left, Db2FilterExpression Right
├── Db2OrFilterExpression               # left OR right
│       Properties: Db2FilterExpression Left, Db2FilterExpression Right
├── Db2NotFilterExpression              # NOT inner
│       Properties: Db2FilterExpression Inner
├── Db2NullCheckFilterExpression        # field IS NULL / IS NOT NULL
│       Properties: Db2FieldAccess Field, bool IsNotNull
└── Db2NavigationSemiJoinFilterExpression  # WHERE nav.Field == value (cross-table semi-join)
        Properties: Db2NavigationJoinPlan JoinPlan, Db2FilterExpression TargetFilter

Db2FieldAccessExpression : Expression
    Properties: Db2FieldSchema Field, int FieldIndex, Type ClrType

Db2ProjectionExpression : Expression
    Properties: Db2FieldAccessExpression Field, string Alias

Db2OrderingExpression
    Properties: Db2FieldAccessExpression Field, bool Ascending
```

**Untranslatable expressions**: When the translator encounters a predicate or expression it cannot represent as a `Db2FilterExpression` (e.g., complex method calls, computed expressions), it should signal this so the compilation step can evaluate them client-side. The standard EF Core pattern is to return `QueryCompilationContext.NotTranslatedExpression`, which causes the base class to generate a client-side evaluation.

### 3.3 `Db2ExpressionTranslator` — Translate CLR Expressions to Db2 Filter Expressions

**Location**: `src/MimironSQL.EntityFrameworkCore/Db2/Query/Db2ExpressionTranslator.cs`

An `ExpressionVisitor` that translates CLR predicate expressions (from `Where` lambda bodies) into `Db2FilterExpression` trees:

```csharp
internal sealed class Db2ExpressionTranslator : ExpressionVisitor
{
    // Translates: x => x.Id == 123  →  Db2ComparisonFilterExpression(Id, Equal, 123)
    // Translates: x => ids.Contains(x.Id)  →  Db2ContainsFilterExpression(Id, [1,2,3])
    // Translates: x => x.Name.Contains("foo")  →  Db2StringMatchFilterExpression(Name, Contains, "foo")
    // Translates: x => x.Id == 1 || x.Id == 2  →  Db2OrFilterExpression(...)
    // Returns null for untranslatable expressions
    
    public Db2FilterExpression? Translate(LambdaExpression predicate, IEntityType entityType);
}
```

This replaces the current dual approach of `Db2RowPredicateCompiler` + `TryExtractPkIds`. Instead of compiling predicates directly to `Func<TRow, bool>`, the translator produces a declarative tree that can be analyzed at compilation time (e.g., to choose PK lookup vs. full scan).

### 3.4 `Db2QueryableMethodTranslatingExpressionVisitor`

**Location**: `src/MimironSQL.EntityFrameworkCore/Db2/Query/Db2QueryableMethodTranslatingExpressionVisitor.cs`

Inherits from `QueryableMethodTranslatingExpressionVisitor`. Implements the ~30 abstract `TranslateXxx` methods.

```csharp
internal sealed class Db2QueryableMethodTranslatingExpressionVisitor(
    QueryableMethodTranslatingExpressionVisitorDependencies dependencies,
    QueryCompilationContext queryCompilationContext,
    Db2ExpressionTranslator expressionTranslator)
    : QueryableMethodTranslatingExpressionVisitor(dependencies, queryCompilationContext, subquery: false)
{
    protected override ShapedQueryExpression CreateShapedQueryExpression(IEntityType entityType)
    {
        var tableName = entityType.GetTableName() ?? entityType.ClrType.Name;
        var queryExpression = new Db2QueryExpression(entityType, tableName);
        
        return new ShapedQueryExpression(
            queryExpression,
            new StructuralTypeShaperExpression(
                entityType,
                new ProjectionBindingExpression(queryExpression, new ProjectionMember(), typeof(ValueBuffer)),
                nullable: false));
    }
    
    protected override ShapedQueryExpression? TranslateWhere(ShapedQueryExpression source, LambdaExpression predicate)
    {
        var db2Query = (Db2QueryExpression)source.QueryExpression;
        var filter = _translator.Translate(predicate, db2Query.EntityType);
        
        if (filter is null)
            return null;  // Not translatable → EF Core will generate client eval warning or fail
        
        db2Query.ApplyFilter(filter);
        return source;
    }
    
    protected override ShapedQueryExpression TranslateSelect(ShapedQueryExpression source, LambdaExpression selector)
    {
        // Update the shaper expression; the actual projection happens at compilation time.
        // Follow InMemory's pattern of updating the projection mapping.
        ...
    }
    
    protected override ShapedQueryExpression? TranslateFirstOrDefault(
        ShapedQueryExpression source, LambdaExpression? predicate, Type returnType, bool returnDefault)
    {
        if (predicate is not null)
        {
            source = TranslateWhere(source, predicate);
            if (source is null) return null;
        }
        
        var db2Query = (Db2QueryExpression)source.QueryExpression;
        db2Query.ApplyLimit(1);
        
        return source.UpdateResultCardinality(
            returnDefault ? ResultCardinality.SingleOrDefault : ResultCardinality.Single);
    }
    
    // Initially unsupported methods return null (causes EF Core to throw "could not be translated"):
    protected override ShapedQueryExpression? TranslateGroupBy(...) => null;
    protected override ShapedQueryExpression? TranslateJoin(...) => null;
    protected override ShapedQueryExpression? TranslateGroupJoin(...) => null;
    protected override ShapedQueryExpression? TranslateUnion(...) => null;
    protected override ShapedQueryExpression? TranslateIntersect(...) => null;
    protected override ShapedQueryExpression? TranslateExcept(...) => null;
    // etc.
}
```

**Methods to implement in Phase 1** (see Section 6):
- `CreateShapedQueryExpression` — creates `Db2QueryExpression` for an entity type
- `TranslateWhere` — translates predicates to `Db2FilterExpression`
- `TranslateSelect` — updates projection mapping
- `TranslateFirstOrDefault` / `TranslateSingleOrDefault` — applies limit + cardinality
- `TranslateCount` / `TranslateAny` / `TranslateAll` — scalar aggregates
- `TranslateTake` / `TranslateSkip` — pagination
- `TranslateOrderBy` / `TranslateThenBy` — ordering
- `TranslateDistinct`
- `CreateSubqueryVisitor`

**Methods to defer** (return `null`):
- `TranslateGroupBy`, `TranslateJoin`, `TranslateGroupJoin`, `TranslateLeftJoin`
- `TranslateUnion`, `TranslateIntersect`, `TranslateExcept`, `TranslateConcat`
- `TranslateSelectMany`
- `TranslateAverage`, `TranslateSum`, `TranslateMin`, `TranslateMax`
- `TranslateContains` (the set operation, not method)

### 3.5 `Db2ShapedQueryCompilingExpressionVisitor`

**Location**: `src/MimironSQL.EntityFrameworkCore/Db2/Query/Db2ShapedQueryCompilingExpressionVisitor.cs`

Inherits from `ShapedQueryCompilingExpressionVisitor`. Compiles `Db2QueryExpression` + shaper into executable code.

```csharp
internal sealed class Db2ShapedQueryCompilingExpressionVisitor(
    ShapedQueryCompilingExpressionVisitorDependencies dependencies,
    QueryCompilationContext queryCompilationContext)
    : ShapedQueryCompilingExpressionVisitor(dependencies, queryCompilationContext)
{
    protected override Expression VisitShapedQuery(ShapedQueryExpression shapedQueryExpression)
    {
        var db2Query = (Db2QueryExpression)shapedQueryExpression.QueryExpression;
        db2Query.ApplyProjection();
        
        // Inject EF Core's standard entity materializers.
        var shaperBody = InjectStructuralTypeMaterializers(shapedQueryExpression.ShaperExpression);
        
        // Replace ProjectionBindingExpression nodes with ValueBuffer access.
        shaperBody = new Db2ProjectionBindingRemovingExpressionVisitor(db2Query).Visit(shaperBody);
        
        // Compile the shaper into a Func<ValueBuffer, TResult>.
        var shaperLambda = Expression.Lambda(shaperBody, Db2QueryExpression.ValueBufferParameter);
        
        // Build the runtime enumerable expression.
        // This produces code equivalent to:
        //   new Db2QueryingEnumerable<TResult>(queryContext, db2Query, shaperFunc)
        
        var enumerableType = typeof(Db2QueryingEnumerable<>).MakeGenericType(shaperLambda.ReturnType);
        var constructor = enumerableType.GetConstructors().Single();
        
        return Expression.New(
            constructor,
            QueryCompilationContext.QueryContextParameter,
            Expression.Constant(db2Query.CreateExecutionPlan()),
            shaperLambda);
    }
}
```

### 3.6 `Db2QueryExecutionPlan` — Runtime Execution Strategy

**Location**: `src/MimironSQL.EntityFrameworkCore/Db2/Query/Db2QueryExecutionPlan.cs`

A serializable plan that `Db2QueryingEnumerable` uses at runtime to read from `IDb2File`. This separates compilation-time analysis from runtime execution.

```csharp
internal sealed class Db2QueryExecutionPlan
{
    public string TableName { get; init; }
    public IEntityType EntityType { get; init; }
    
    // Pre-analyzed execution strategy:
    public Db2ExecutionStrategy Strategy { get; init; }
    // One of: FullScan, PrimaryKeyLookup, PrimaryKeyMultiLookup
    
    // For PK lookups:
    public IReadOnlyList<int>? PrimaryKeyIds { get; init; }
    
    // Compiled row-level filter (for predicates that map to field reads):
    public Db2FilterExpression? Filter { get; init; }
    
    // Projection info (which fields to read, in what order):
    public IReadOnlyList<Db2ProjectionExpression> Projections { get; init; }
    
    // Pagination:
    public int? Limit { get; init; }
    public int? Offset { get; init; }
}

internal enum Db2ExecutionStrategy
{
    FullScan,               // Enumerate all rows, apply filter
    PrimaryKeyLookup,       // Single ID lookup via TryGetRowById
    PrimaryKeyMultiLookup,  // Multiple ID lookup via TryGetRowById
}
```

The key insight: during compilation, the `Db2QueryExpression.CreateExecutionPlan()` method analyzes the filter tree. If it's purely PK-based (equality or Contains), it emits a `PrimaryKeyLookup`/`PrimaryKeyMultiLookup` strategy. Otherwise it emits `FullScan` with a compiled filter delegate.

### 3.7 `Db2QueryingEnumerable<T>` — Runtime Enumerable

**Location**: `src/MimironSQL.EntityFrameworkCore/Db2/Query/Db2QueryingEnumerable.cs`

The runtime component that executes DB2 reads. Injected into the compiled query delegate.

```csharp
internal sealed class Db2QueryingEnumerable<T>(
    QueryContext queryContext,
    Db2QueryExecutionPlan plan,
    Func<ValueBuffer, T> shaper) : IEnumerable<T>, IAsyncEnumerable<T>
{
    public IEnumerator<T> GetEnumerator()
    {
        // Resolve services from QueryContext.
        var store = queryContext.Context.GetService<IMimironDb2Store>();
        var (file, schema) = store.OpenTableWithSchema(plan.TableName);
        
        return plan.Strategy switch
        {
            Db2ExecutionStrategy.PrimaryKeyLookup => EnumeratePkLookup(file, plan, shaper),
            Db2ExecutionStrategy.PrimaryKeyMultiLookup => EnumeratePkMultiLookup(file, plan, shaper),
            Db2ExecutionStrategy.FullScan => EnumerateFullScan(file, plan, shaper),
            _ => throw new NotSupportedException()
        };
    }
    
    private IEnumerator<T> EnumerateFullScan(IDb2File file, Db2QueryExecutionPlan plan, Func<ValueBuffer, T> shaper)
    {
        var filter = plan.Filter?.Compile();  // Compile Db2FilterExpression → Func<IDb2File, RowHandle, bool>
        var yielded = 0;
        var skipped = 0;
        
        foreach (var handle in file.EnumerateRowHandles())
        {
            if (filter is not null && !filter(file, handle))
                continue;
            
            if (plan.Offset.HasValue && skipped < plan.Offset.Value)
            {
                skipped++;
                continue;
            }
            
            // Build ValueBuffer from projected fields.
            var values = new object?[plan.Projections.Count];
            for (var i = 0; i < plan.Projections.Count; i++)
            {
                var proj = plan.Projections[i];
                values[i] = file.ReadField<object>(handle, proj.Field.FieldIndex);
            }
            
            yield return shaper(new ValueBuffer(values));
            
            yielded++;
            if (plan.Limit.HasValue && yielded >= plan.Limit.Value)
                yield break;
        }
    }
}
```

### 3.8 Factory Types

Each pipeline stage needs a factory registered in DI:

| Factory Interface | Implementation |
|---|---|
| `IQueryableMethodTranslatingExpressionVisitorFactory` | `Db2QueryableMethodTranslatingExpressionVisitorFactory` |
| `IShapedQueryCompilingExpressionVisitorFactory` | `Db2ShapedQueryCompilingExpressionVisitorFactory` |
| `IQueryTranslationPreprocessorFactory` | `Db2QueryTranslationPreprocessorFactory` |
| `IQueryContextFactory` | `Db2QueryContextFactory` |

### 3.9 `Db2ProjectionBindingRemovingExpressionVisitor`

**Location**: `src/MimironSQL.EntityFrameworkCore/Db2/Query/Db2ProjectionBindingRemovingExpressionVisitor.cs`

Rewrites `ProjectionBindingExpression` nodes (produced by EF Core's standard materializer injection) into `ValueBuffer` index accesses. This is how the shaper knows which slot in the `ValueBuffer` corresponds to which property.

---

## 4. Types to Remove (After Full Migration)

Once the new pipeline is fully functional and all tests pass:

| Type | Reason for Removal |
|---|---|
| `MimironDb2QueryExecutor` | Replaced by standard `QueryCompiler` + pipeline |
| `MimironDb2EfCoreInternalServiceRegistration` | No longer replacing `IQueryCompiler` |
| `MimironDb2EfExpressionNormalizer` | EF Core's preprocessor handles normalization |
| `Db2ExpressionPreprocessor` | Include/AsNoTracking handled by EF Core's `NavigationExpandingExpressionVisitor` |
| `MimironDb2AsyncQueryAdapter` | Standard EF Core handles async wrapping |
| `Db2QueryProvider` | The entire custom `IQueryProvider` is replaced by the pipeline |
| `Db2Queryable` | No longer needed; EF Core uses its own `EntityQueryable<T>` |
| `Db2IncludePolicy` | EF Core enforces Include policy natively |
| `Db2RowPredicateCompiler` | Replaced by `Db2ExpressionTranslator` + compilation in `Db2QueryingEnumerable` |
| `Db2RowProjectorCompiler` | Replaced by `Db2QueryExpression` projection + ValueBuffer |
| `Db2RowHandleAccess` | Replaced by direct `ReadField<T>` calls in compiled shaper |
| `Db2RequiredColumns` | Column pruning handled by `Db2QueryExpression` projection |
| `Db2SourceRequirements` | Merged into `Db2QueryExpression` |
| `RootQueryRewriter` | No longer substituting query roots manually |
| `OperationStripper` | No longer stripping operations from expression trees |
| `EfPropertyToMemberAccessRewriter` | No longer needed — EF Core's own materializer handles `EF.Property()` |

### Types to Keep/Evolve

| Type | Comment |
|---|---|
| `Db2EntityMaterializer` | May evolve to work with ValueBuffer instead of direct ReadField calls; OR replaced by EF Core's `StructuralTypeMaterializerSource` |
| `Db2EntityMaterializerCache` | Same as above |
| `Db2IncludeChainExecutor` | May be replaced by EF Core's Include infrastructure; needs investigation |
| `Db2NavigationQueryTranslator` | Migrated into `Db2ExpressionTranslator` for semi-join filter translation |
| `Db2NavigationQueryCompiler` | Runtime execution of semi-join filters; integrated into `Db2QueryingEnumerable` |
| `Db2NavigationQueryPlan` | Plan types reused by `Db2NavigationSemiJoinFilterExpression` |
| `Db2DenseStringScanner` / `Db2DenseStringMatch` | Reused by string filter compilation in `Db2QueryingEnumerable` |
| `Db2ModelBinding` / `Db2EntityType` | Keep — core model binding; may need adaptation for EF Core's `IEntityType` |
| `Db2TableSchema` / `Db2FieldSchema` / `SchemaMapper` | Keep — physical schema types |
| `IMimironDb2Store` / `MimironDb2Store` | Keep — store abstraction |
| `MimironDb2Database` | Update — change `CompileQuery` to use `QueryCompilationContext.CreateQueryExecutor` |

---

## 5. Service Registration Changes

### Current (`MimironDb2ServiceCollectionExtensions.AddCoreServices`)

```csharp
new EntityFrameworkServicesBuilder(services).TryAddCoreServices();
services.Replace(ServiceDescriptor.Scoped<IQueryCompiler, MimironDb2QueryExecutor>());  // ← REMOVE
services.TryAddScoped<IDatabase, MimironDb2Database>();
// ... other services
```

### Proposed

```csharp
new EntityFrameworkServicesBuilder(services).TryAddCoreServices();

// ── Query pipeline (NEW) ──
services.TryAddScoped<IQueryableMethodTranslatingExpressionVisitorFactory,
    Db2QueryableMethodTranslatingExpressionVisitorFactory>();
services.TryAddScoped<IShapedQueryCompilingExpressionVisitorFactory,
    Db2ShapedQueryCompilingExpressionVisitorFactory>();
services.TryAddScoped<IQueryTranslationPreprocessorFactory,
    Db2QueryTranslationPreprocessorFactory>();
services.TryAddScoped<IQueryContextFactory,
    Db2QueryContextFactory>();

// ── Remove IQueryCompiler replacement ──
// MimironDb2EfCoreInternalServiceRegistration.Add(services);  // ← REMOVE

// ── Keep existing ──
services.TryAddScoped<IDatabase, MimironDb2Database>();
services.TryAddSingleton<ITypeMappingSource, MimironDb2TypeMappingSource>();
// ... etc.
```

### `MimironDb2Database.CompileQuery` Update

Currently `CompileQuery` manually resolves `IQueryCompiler` at execution time. With the standard pipeline:

```csharp
internal sealed class MimironDb2Database(
    IQueryCompilationContextFactory queryCompilationContextFactory) : IDatabase
{
    public Func<QueryContext, TResult> CompileQuery<TResult>(Expression query, bool async)
    {
        var context = queryCompilationContextFactory.Create(async);
        return context.CreateQueryExecutor<TResult>(query);
    }
    
    // ... SaveChanges throws NotSupportedException
}
```

This is the standard pattern used by InMemory. The compiled delegate is cached by EF Core's `CompiledQueryCache`.

---

## 6. Phased Implementation Plan

### Phase 1: Minimal Pipeline (Basic Where + Select + First)

**Goal**: Get a simple query like `db.Creatures.Where(x => x.Id == 1).FirstOrDefault()` working through the standard pipeline.

**Files to create**:
1. `Db2/Query/Expressions/Db2QueryExpression.cs` — minimal: entity type, table name, filter, limit
2. `Db2/Query/Expressions/Db2FilterExpression.cs` — abstract base + `Db2ComparisonFilterExpression`
3. `Db2/Query/Expressions/Db2FieldAccessExpression.cs`
4. `Db2/Query/Db2ExpressionTranslator.cs` — translates `x.Id == 1` 
5. `Db2/Query/Db2QueryableMethodTranslatingExpressionVisitor.cs` — `CreateShapedQueryExpression`, `TranslateWhere`, `TranslateFirstOrDefault`
6. `Db2/Query/Db2ShapedQueryCompilingExpressionVisitor.cs` — `VisitShapedQuery` with `Db2QueryingEnumerable`
7. `Db2/Query/Db2QueryingEnumerable.cs` — runtime enumeration with full scan + PK lookup
8. `Db2/Query/Db2ProjectionBindingRemovingExpressionVisitor.cs`
9. 4 factory classes
10. `Db2/Query/Db2QueryContextFactory.cs` + `Db2QueryContext.cs`

**Files to modify**:
1. `MimironDb2ServiceCollectionExtensions.cs` — register new factories, remove `IQueryCompiler` replacement
2. `MimironDb2Database.cs` — use `IQueryCompilationContextFactory`

**Expected test impact**: Many tests will break initially because the old pipeline code paths are removed. Fix forward.

### Phase 2: Rich Filters + PK Optimization

**Goal**: Support complex Where predicates and automatic PK lookup optimization.

- Add remaining filter expression types: `Db2ContainsFilterExpression`, `Db2StringMatchFilterExpression`, `Db2AndFilterExpression`, `Db2OrFilterExpression`, `Db2NullCheckFilterExpression`
- Implement PK detection in `Db2QueryExpression.CreateExecutionPlan()` — analyze filter tree for PK equality/Contains patterns
- Implement `TranslateCount`, `TranslateAny`, `TranslateAll`
- Implement `TranslateTake`, `TranslateSkip`

### Phase 3: Projections

**Goal**: Support `.Select(x => new { x.Id, x.Name })` without materializing full entities.

- Full projection mapping in `Db2QueryExpression`
- ValueBuffer population for projected fields only
- `TranslateSelect` implementation
- Column pruning (only read projected fields from DB2 file)

### Phase 4: Navigation + Include

**Goal**: Support `.Include(x => x.Faction)` and navigation-based Where predicates.

- Investigate how EF Core's `NavigationExpandingExpressionVisitor` transforms Include calls
- Implement `Db2NavigationSemiJoinFilterExpression` for cross-table predicates
- Implement include materialization in `Db2QueryingEnumerable` (or via EF Core's include infrastructure)
- Migrate logic from `Db2IncludeChainExecutor` and `Db2NavigationQueryCompiler`

### Phase 5: Ordering + Distinct

- `TranslateOrderBy`, `TranslateThenBy` — sort results in-memory after DB2 scan (DB2 files have no natural index-based ordering beyond row order)
- `TranslateDistinct` — in-memory deduplication
- `TranslateReverse`

### Phase 6: Cleanup

- Remove all types listed in Section 4
- Remove `#pragma warning disable EF1001` suppressions
- Update tests to use standard EF Core patterns
- Performance benchmarks comparing old vs. new pipeline

---

## 7. Key Design Decisions

### 7.1 ValueBuffer vs. Direct ReadField

**Decision**: Use `ValueBuffer` as the row abstraction, following InMemory's pattern.

**Rationale**: EF Core's `StructuralTypeShaperExpression` and `ProjectionBindingExpression` are designed to work with `ValueBuffer`. Using it means we get EF Core's standard entity materializer for free (identity resolution, change tracking, lazy loading proxies). The cost is an extra allocation per row, but it eliminates the need to maintain a custom `Db2EntityMaterializer`.

### 7.2 Client vs. Server Evaluation

**Decision**: All filtering that maps to `Db2FieldSchema` operations is "server-side" (pushed to the DB2 file reader). Everything else is client-evaluated.

DB2 files support:
- **Field equality** — compare field value at known column index
- **Field range comparisons** — numeric comparisons
- **PK lookup** — `TryGetRowById` for O(log n) access
- **Dense string matching** — Contains/StartsWith/EndsWith via string table scan
- **Full scan** — iterate all rows

DB2 files do NOT natively support:
- Joins (each table is a separate file)
- Aggregation (SUM, AVG, etc.)
- DISTINCT
- ORDER BY (rows are in section/record order)

Operations that cannot be pushed down should be handled by EF Core's client evaluation mechanism. When `TranslateXxx` returns `null`, EF Core will either client-evaluate (if configured) or throw a translation error.

### 7.3 Navigation Semi-Joins

**Decision**: Model cross-table predicates as `Db2NavigationSemiJoinFilterExpression` in the filter tree.

When a query has `Where(x => x.Faction.Name == "Horde")`, this involves reading from a different DB2 table (Faction). The current approach scans the target table, collects matching IDs, then filters the root table. This same strategy should be represented as a first-class filter expression that the `Db2QueryingEnumerable` executes at runtime.

### 7.4 Async Support

**Decision**: Wrap sync execution in `Task.FromResult` / `ValueTask`, same as InMemory provider.

DB2 file reads are fully synchronous (memory-mapped or buffered I/O). The async variants (`IAsyncEnumerable<T>`) simply wrap the synchronous enumerator. This is the same approach InMemory uses.

### 7.5 Compiled Query Cache

**Decision**: Use EF Core's standard `CompiledQueryCache` (singleton).

One major benefit of adopting the standard pipeline: queries are compiled once and cached. The current approach re-analyzes the expression tree on every `Execute<TResult>` call. With the standard pipeline, `QueryCompiler` checks the `CompiledQueryCache` first, and only calls `IDatabase.CompileQuery` on a cache miss.

---

## 8. Risk Assessment

| Risk | Severity | Mitigation |
|---|---|---|
| EF Core internal API changes | Medium | Pin to stable EF Core version; avoid `[InternalApi]` where possible |
| Performance regression from ValueBuffer allocation | Low | DB2 files are small (thousands of rows); allocation cost negligible vs. I/O |
| Include behavior differs from current custom implementation | Medium | Extensive test coverage for Include scenarios; compare behavior |
| Navigation expansion produces unexpected expression shapes | Medium | Start with simple cases; add comprehensive logging |
| Loss of dense string optimization | Low | Reimplement as Db2StringMatchFilterExpression + Db2DenseStringScanner |
| Breaking all tests simultaneously | High | Accept temporary breakage; fix forward per phase |
| EF Core's preprocessor strips/transforms nodes we depend on | Medium | Custom `Db2QueryTranslationPreprocessor` can opt out of specific transformations |

---

## 9. References

- [EF Core Query Pipeline (source)](https://github.com/dotnet/efcore/tree/main/src/EFCore/Query)
- [InMemory Provider (source)](https://github.com/dotnet/efcore/tree/main/src/EFCore.InMemory)
- [Cosmos Provider (source)](https://github.com/dotnet/efcore/tree/main/src/EFCore.Cosmos)
- [QueryCompilationContext.CreateQueryExecutorExpression](https://github.com/dotnet/efcore/blob/main/src/EFCore/Query/QueryCompilationContext.cs)
- [DB2 file structure](https://wowdev.wiki/DB2)
