# Plan: Remaining Test Fixes

This plan addresses the 4 failing EF Core unit tests and 18 failing integration tests after the Phase 1-3 query pipeline migration.

## Current Status

| Suite | Passed | Failed | Total |
|---|---|---|---|
| EF Core | 245 | 4 | 249 |
| Integration | 7 | 18 | 25 |

**EF Core failures (4):** All navigation/include (Phase 4)
- ThenInclude × 2, AutoInclude, LazyLoading

**Integration failures (18):**
- 15 navigation/include tests (Phase 4)
- 3 advanced query features (`string.Length`, aggregate terminals, collection predicates)

---

## Phase 4A: Quick Wins (Non-Navigation)

### 1. Add `string.Length` Translation
**Tests fixed:** `Can_query_locstring_tables_via_filesystem_provider`

**Location:** `Db2ExpressionTranslator.cs`

**Change:** In `TryResolveFieldAccess`, handle member access on a field (e.g., `x.Name.Length`). When the member is `string.Length`, wrap the field in a new `Db2StringLengthFieldExpression` (or equivalent), and support *all* comparison operators in `TranslateBinary` (`==`, `!=`, `>`, `>=`, `<`, `<=`). This should give a general-purpose translation of `Length` rather than only handling `> 0`.

### 2. Fix Exception Type for Navigation Projection
**Tests fixed:** `Selecting_navigation_entity_in_pruned_row_projection_throws`

**Location:** `Db2QueryableMethodTranslatingExpressionVisitor.cs` or `Db2ShapedQueryCompilingExpressionVisitor.cs`

**Change:** When a navigation property is detected in a projection (not yet supported), throw `NotSupportedException` explicitly instead of letting EF Core's generic `InvalidOperationException` bubble up. Add a check in `TranslateSelect` or `VisitShapedQuery` for navigation member access.

---

## Phase 4B: Navigation Support (Core)

### 3. Include/ThenInclude Support
**Tests fixed:** 4 EF Core unit tests + ~12 integration tests

**Overview:** EF Core's `IncludeExpression` nodes appear in the query tree after navigation expansion. Our pipeline needs to:

1. **Recognize `IncludeExpression`** in the shaper expression tree
2. **Store include paths** in `Db2QueryExpression` 
3. **Post-process results** to load related entities via **fully batched strategies**, minimizing per-entity round-trips

**Implementation approach (immediately fully batched):**

a. **Add `Db2IncludeExpression`** to track navigation paths:
```csharp
internal sealed record Db2IncludeExpression(
    INavigation Navigation,
    Expression? NavigationExpression);
```

b. **Override `TranslateInclude`** in `Db2QueryableMethodTranslatingExpressionVisitor`:
- Store the navigation path in `Db2QueryExpression.Includes`
- Return the source with updated shaper

c. **Modify `BuildShaper<T>` / query execution to handle includes in batched form:**
- After materializing the root entities, collect all relevant FK/PK values for each include path.
- For **reference navigations**, execute a *single* batched query per navigation that loads all needed dependents into an in-memory index keyed by FK/PK, then hydrate properties on the root entities from that index.
- For **collection navigations**, execute a *single* batched query per collection navigation that loads all dependents and groups them by parent key, then assign the grouped lists to the collection properties.
- This should avoid per-entity queries and approximate EF's relational Include behavior, within DB2 constraints.

### 4. Navigation Property Filter Translation
**Tests fixed:** Navigation filter tests (`x => x.Navigation.Property == ...`)

**Location:** `Db2ExpressionTranslator.cs`

**Change:** In `TryResolveFieldAccess`, when the member expression chain has multiple levels (e.g., `x.Map.Directory`):
- Detect navigation property access via EF metadata (`INavigation` / `IForeignKey`).
- Implement a **general navigation filter translation layer** that turns such access into explicit join- or subquery-based filters over DB2 tables (e.g., via EXISTS subqueries or key-based lookups), not only for patterns used in current tests.
- Extend tests over time to exercise additional navigation filter shapes and verify this general behavior.

### 5. Collection Navigation Predicates
**Tests fixed:** `x => x.MapChallengeModes.Count > 0`, `x => x.Collection.Any()` and more in future

**Location:** `Db2ExpressionTranslator.cs` + new subquery support

**Change (general layer):** Recognize collection navigation expressions and translate them into EXISTS-style filters:
- `navigation.Count > 0` → EXISTS subquery on the related table
- `navigation.Any()` → EXISTS subquery  
- `navigation.Any(predicate)` → EXISTS subquery with predicate applied in the subquery

This requires:
- Detecting `MemberExpression` / `MethodCallExpression` where the member is a collection navigation.
- Translating to `Db2ExistsSubqueryFilterExpression(relatedTableName, foreignKeyName, principalKeyField)` (or equivalent representation) that can support additional patterns beyond those in current tests.
- Runtime execution opens the related table, applies the subquery filter, and checks if any rows match.

---

## Phase 4C: Advanced Features

### 6. Terminal Operator Semantics
**Tests fixed:** `Can_execute_all_and_single_or_default_terminal_operators`

Implement `TranslateAll`, `TranslateAny`, and `TranslateCount` with semantics matching EF Core as closely as possible over DB2:
- `Count()` → server-side-compatible enumeration of all matches with count aggregation.
- `All(predicate)` → translate to `!source.Any(!predicate)` when possible, using the existing/extended translation layer.
- `Any(predicate)` → leverage existing filter + limit pipeline for efficient existence checks.
Avoid relying on implicit client evaluation for these operators; aim for provider-level behavior that aligns with EF Core expectations.

### 7. AutoInclude and Lazy Loading
**Tests fixed:** 2 EF Core unit tests

**Depends on:** Include support (item 3)

**AutoInclude:** EF Core adds `IncludeExpression` automatically when configured with `AutoInclude()`. The provider should honor these to the same extent as EF relational providers, using the same include pipeline (and batched strategies) rather than treating them as best-effort hints.

**Lazy Loading:**
- Intercept property access on proxy entities and load navigations on demand via `ILazyLoader` or equivalent.
- Ensure behavior matches EF Core’s lazy-loading semantics (e.g., single-load per navigation instance, respect of tracking / change tracker rules) as far as possible given DB2 as the backing store.
- Tests should be able to rely on EF-style lazy loading, not a simplified approximation.

---

## Recommended Execution Order

| Order | Task | Complexity | Tests Fixed |
|-------|------|------------|-------------|
| 1 | `string.Length` translation | Low | 1 |
| 2 | Navigation projection exception type | Low | 1 |
| 3 | Collection `.Any()` / `.Count > 0` → EXISTS | Medium | 3-4 |
| 4 | Basic Include (reference) | Medium | 4-6 |
| 5 | Include (collection) + ThenInclude | Medium | 4-6 |
| 6 | Navigation filter translation | High | 6+ |
| 7 | AutoInclude + Lazy Loading | Medium | 2 |

**Estimated effort:** Items 1-2 are ~30 mins each. Items 3-7 are Phase 4 proper (several hours total).
