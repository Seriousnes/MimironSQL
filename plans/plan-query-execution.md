# Plan: Native DB2 Query Execution (Where / Find / Include)

## Status: Draft
## Date: 2026-02-15

---

## 1. Definitions (DB2 Context)

In this repository, DB2 is **relational but not SQL**.

- **Server-side (native DB2 ops):** Query logic executed directly over DB2 tables via `IDb2File` primitives (scan/lookup + `ReadField<T>`), without LINQ-to-Objects over materialized rows/entities.
- **Client-side:** LINQ-to-Objects or compiled delegates executed over already materialized rows/entities.

Both occur in-process; the distinction is **native DB2 ops vs post-materialization LINQ**.

---

## 2. Goals

- Minimize table reads by pushing as much filtering and relationship loading as practical into native DB2 operations.
- Make the following operations native DB2 ops (first priority):
  - `Where`
  - `Find`
  - `Include` (reference, collection, FK-array modeled navigations)
- Keep semantics aligned with EF Core conventions, including **tracking-correct** relationship loading/fixup.
- Avoid inventing a full SQL engine; implement only what tests and realistic usage demand.

## 3. Non-goals (for now)

- Full LINQ coverage.
- Full server-side translation of arbitrary .NET methods and complex expression constructs.
- Async query execution.

---

## 4. Constraints and Principles

- **Correctness-first** during bootstrap.
- Prefer Cosmos/InMemory patterns for provider architecture.
- Pushdown should never change query semantics.

### 4.1 Safe Partial Pushdown Rules

- For conjunction (`A && B`):
  - Push down any translatable parts.
  - Evaluate remaining parts client-side **after** the reduced row set is materialized.
- For disjunction (`A || B`):
  - Do not partially push down only one side.
  - Either translate the full `||` server-side, or evaluate the whole disjunction client-side.

---

## 5. Building Blocks Already Present

- Schema mapping, including virtual `Id` via `Db2VirtualFieldIndex.Id`.
- DB2 row access primitives:
  - `IDb2File.EnumerateRowHandles()`
  - `IDb2File.TryGetRowHandle(id, out handle)` (when the key maps to DB2's virtual ID)
  - `IDb2File.ReadField<T>(handle, fieldIndex)`
- EF model binding for PK + navigations (`Db2ModelBinding`).

The plan aims to avoid changes to `IDb2Format` unless a concrete missing capability is discovered.

---

## 6. Implementation Plan (Internal Execution Layer)

### 6.1 Shared Per-Provider Caches (Internal Service)

Add a provider-specific singleton service to cache lookup structures across contexts ("per internal shared service"), keyed by:
- WOW version
- table name
- layout hash
- field index (for non-PK indexes)

Cache contents should avoid holding open streams/files; store stable identifiers (e.g., row IDs) rather than `RowHandle` when needed.

Examples:
- Non-Id PK index: `keyValue -> rowId(s)`
- Dependent FK grouping for collection includes: `principalKey -> dependentRowId(s)`

WDC5 already provides efficient virtual-ID lookups and can lazily build its ID index; the cache is primarily for non-Id keys and FK grouping.

### 6.2 Native `Where` Pushdown (DB2 Predicate Evaluator)

Translate supported predicate shapes into a row-handle predicate that:
- reads only the referenced columns via `ReadField<T>`
- filters before materializing full entities/ValueBuffers

**MVP supported shapes**
- Comparisons: `==`, `!=`, `<`, `<=`, `>`, `>=`
- Null checks
- Boolean ops: `!`, `&&`, `||` (with the safe pushdown rules above)
- String ops: `StartsWith`, `Contains`, `EndsWith` using `StringComparison.OrdinalIgnoreCase`
- `Contains`/IN against in-memory sets:
  - If the field is the target entity PK and maps to the DB2 virtual `Id`, implement as N lookups via `TryGetRowHandle`.
  - Otherwise implement as a scan reading only that field and checking membership in a `HashSet<T>`.

**Unsupported server-side (allowed client-side post-materialization)**
- Arbitrary .NET methods
- Complex expression constructs (conditionals, switch, non-whitelisted method calls, computed expressions beyond the supported set)

### 6.3 Native `Find`

Implement `Find` as native DB2 ops by using EF metadata from `Db2ModelBinding`:

- If the EF primary key field schema maps to `Db2VirtualFieldIndex.Id`:
  - Use `IDb2File.TryGetRowHandle(key, out handle)` and materialize exactly one row.
- Otherwise:
  - Use a cached index if available; else scan reading only the PK field and build the index lazily.

### 6.4 Native `Include` (Tracking-Correct)

Includes should be executed using native DB2 reads/lookup while preserving EF Core tracking semantics.

#### 6.4.1 Tracking-correct requirements
- Identity resolution for tracking queries.
- Relationship fixup consistent with EF Core conventions.
- `IsLoaded` should be set so EF does not trigger redundant loading.

#### 6.4.2 Reference navigations (many-to-one / one-to-one)
- Read FK field from principal rows.
- Resolve target rows by target PK:
  - virtual `Id` -> `TryGetRowHandle`
  - otherwise -> cached index / scan
- Materialize targets and ensure they are tracked/identity-resolved.

#### 6.4.3 Collection navigations (one-to-many)
- Collect principal key values.
- Scan dependent table reading only the dependent FK field; group matches.
- Materialize dependent rows, track them, and rely on EF fixup to wire collections.

#### 6.4.4 FK-array modeled navigations
- Read the FK ID array field from the principal row.
- Treat IDs as target PK values (int row IDs + sentinel).
- Batch resolve distinct IDs to target rows via `Find` strategy.
- Track and fix up.

#### 6.4.5 No-tracking behavior
Follow EF Core defaults:
- `AsNoTracking()`: no identity resolution.
- `AsNoTrackingWithIdentityResolution()`: identity resolution without tracking.

---

## 7. Relationship to Existing Plans

- Complements `plans/plan-ef-core-query-pipeline.md` by specifying **native execution semantics** and the pushdown strategy.
- Intersects `plans/plan-remaining-test-fixes.md` in the navigation/include space; this plan focuses on minimal native ops for Where/Find/Include to reduce reads, while preserving EF tracking semantics.

---

## 8. Acceptance Criteria (Bootstrap)

- `Where` reduces row reads: filters are evaluated before materialization whenever translatable.
- `Find` reads at most one row for virtual-ID keys.
- `Include` works without forcing EF Core to generate unsupported translation shapes (e.g., provider should not depend on `SelectMany` translation for FK-array includes).
- The earliest failing test should move forward after each iteration.
