## String Predicate Pushdown + Include Cache & Cleanup

Two independent workstreams that can be implemented in either order. (A) Add native DB2-level string method evaluation (`StartsWith`/`EndsWith`/`Contains`) to the row predicate compiler so string filters reduce row reads before materialization. (B) Introduce a per-provider singleton FK grouping cache to eliminate repeated dependent-table scans across `DbContext` instances, and delete the dead `IncludeExpressionRemovingVisitor` legacy N+1 path.

---

### Workstream A: String Predicate Pushdown

The infrastructure is already in place — `ReadField<string>()` works in WDC5, and `TryTranslateFieldAccess` already handles string properties. The only gap is a `MethodCallExpression` branch in `TryTranslateConjunct`.

**Steps**

1. In [Db2RowPredicateCompiler.cs](src/MimironSQL.EntityFrameworkCore/Query/Internal/Db2RowPredicateCompiler.cs), add a new private method `TryTranslateStringMethodCall` that handles `MethodCallExpression` nodes for:
   - `string.StartsWith(string)` and `string.StartsWith(string, StringComparison)`
   - `string.EndsWith(string)` and `string.EndsWith(string, StringComparison)`
   - `string.Contains(string)` and `string.Contains(string, StringComparison)`
   - For each: translate the receiver via `TryTranslateFieldAccess` to get the `ReadField<string>` expression, ensure the argument is a constant/captured variable (not entity-dependent), and emit an `Expression.Call` on the translated string value.

2. In `TryTranslateConjunct` (around [line ~180](src/MimironSQL.EntityFrameworkCore/Query/Internal/Db2RowPredicateCompiler.cs#L180), after the existing `Contains`/IN handling), add a branch: if the expression is a `MethodCallExpression` whose `Method.DeclaringType == typeof(string)` and the method name is `StartsWith`/`EndsWith`/`Contains`, delegate to `TryTranslateStringMethodCall`.

3. Handle `StringComparison` overloads: if the method has a `StringComparison` argument, extract it as a constant and pass it through in the emitted call. If the comparison type is not a constant (e.g., a variable), fall through to client-side evaluation. The plan specifies `OrdinalIgnoreCase` as the primary target — `Ordinal` and `OrdinalIgnoreCase` are the safe server-side comparisons. Other comparison types (`CurrentCulture`, etc.) should fall through to client-side.

4. Guard against the existing `string.Contains` / IN-list ambiguity: the existing guard at [line ~217](src/MimironSQL.EntityFrameworkCore/Query/Internal/Db2RowPredicateCompiler.cs#L217) already rejects `string.Contains(string)` from the IN-list path. The new string method branch should be checked **before** the IN-list branch, or the IN-list guard should route to the string method handler instead of returning `false`.

5. **Tests** — Add unit tests in [MimironSQL.EntityFrameworkCore.Tests](tests/MimironSQL.EntityFrameworkCore.Tests) (new file `Db2/Query/StringPredicatePushdownTests.cs`):
   - `StartsWith` with constant string filters row-level
   - `EndsWith` with constant string filters row-level
   - `Contains(string)` with constant string filters row-level
   - `StringComparison.OrdinalIgnoreCase` overload works
   - Non-entity-dependent argument (captured variable) works
   - Entity-dependent argument falls through to client-side
   - Unsupported `StringComparison` value falls through to client-side
   - Integration test: `ctx.Set<Map>().Where(m => m.Name.StartsWith("Dire")).ToList()` returns correct results

---

### Workstream B: Include Cache & Legacy Removal

#### B1: Delete the Dead `IncludeExpressionRemovingVisitor`

Research confirms this file is **dead code** — it is not referenced anywhere outside its own file. The pipeline uses `IncludeExpressionExtractingVisitor` → `IncludePlan[]` → `Db2IncludeExecutor` instead.

6. Delete [IncludeExpressionRemovingVisitor.cs](src/MimironSQL.EntityFrameworkCore/Query/Internal/Visitor/IncludeExpressionRemovingVisitor.cs). Verify no compilation errors.

#### B2: Per-Provider Singleton FK Grouping Cache

Currently, `ScanOneToMany` in [Db2IncludeExecutor.cs](src/MimironSQL.EntityFrameworkCore/Query/Internal/Db2IncludeExecutor.cs#L486) does a full table scan per query. The plan (section 6.1) calls for a singleton cache storing row IDs so repeated queries skip the scan.

7. Create a new internal class `Db2FkGroupingCache` (singleton lifetime) in the `Query/Internal/` folder. Interface:
   - Key: `(string TableName, string LayoutHash, int ForeignKeyFieldIndex)` — the version can be implicit since the provider is configured per-version.
   - Value: `IReadOnlyDictionary<int, int[]>` mapping `principalKey → dependentRowId[]` (store row IDs, not entities or handles).
   - Method: `GetOrBuild(key, Func<IReadOnlyDictionary<int, int[]>> factory)` — thread-safe, lazily builds the grouping on first access.
   - Use `ConcurrentDictionary` internally.

8. Register `Db2FkGroupingCache` as a **Singleton** in [MimironDb2ServiceCollectionExtensions.cs](src/MimironSQL.EntityFrameworkCore/Extensions/MimironDb2ServiceCollectionExtensions.cs), following the existing `IDbdParser`/`IDb2Format` pattern.

9. Inject `Db2FkGroupingCache` into `Db2IncludeExecutor` (or into `Db2ClientQueryExecutor` which constructs it). Modify `ScanOneToMany` to:
   - Compute the cache key from the dependent table name, layout hash (from `Db2TableSchema`), and FK field index.
   - On cache miss: perform the existing full scan, but store only `principalKey → rowId[]` in the cache (not materialized entities).
   - On cache hit: use the cached row ID grouping to resolve principal IDs → dependent row IDs, then materialize only the needed dependents via ID-based lookups.
   - The cache stores `int[]` row IDs, not `RowHandle` or entity instances, to avoid holding open streams per the plan spec.

10. Ensure the `Db2TableSchema` exposes the layout hash (or add it if missing). Check [Db2TableSchema](src/MimironSQL.EntityFrameworkCore/Db2/Schema/Db2TableSchema.cs) for a `LayoutHash` property.

11. **Tests** — Add tests verifying:
    - Two separate `DbContext` instances querying the same table + Include reuse the cached FK grouping (verify via a counter or mock).
    - Cache correctness: the cached grouping produces the same results as a fresh scan.
    - Thread safety: concurrent access doesn't corrupt the cache.

---

### Verification

- Run `dotnet test MimironSQL.slnx` — all existing tests must continue to pass (except the 1 async skip).
- New string pushdown tests pass.
- New cache tests pass.
- Compilation succeeds after deleting `IncludeExpressionRemovingVisitor.cs`.

### Decisions

- **Workstream order**: A (string pushdown) is smaller and independent — can be done first. B can follow immediately.
- **StringComparison support**: Only `Ordinal` and `OrdinalIgnoreCase` are pushed down server-side. Other comparison types fall through to client-side.
- **Cache granularity**: Cache stores `int[]` row IDs (not entities) per the plan spec, to avoid lifetime/tracking issues across contexts.
- **IncludeExpressionRemovingVisitor**: Straight deletion — confirmed dead code.
