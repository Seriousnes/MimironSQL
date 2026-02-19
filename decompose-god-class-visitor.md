## Decompose the God-Class Visitor (Structural Refactor)

### Rational

1. **The file is a scaling bottleneck.** Every new feature — `OrderBy`, `Skip`, `Distinct`, async, string ops — will land in this one file. At 2,290 lines it's already the hardest file to reason about, and any bug-fix risks regressions in unrelated execution paths.

2. **It conflates 5+ distinct responsibilities:**
   - Expression tree visitation & shaper compilation (the actual `ShapedQueryCompilingExpressionVisitor` role)
   - Row-level predicate analysis + compilation (`TryCompileRowHandlePredicate`, `TryTranslateConjunct`, `TryTranslateFieldAccess`)
   - DB2 table scanning / `ValueBuffer` production (`Table()`, `ValueBufferReadPlan`, joins)
   - Include execution engine (`ExecuteIncludes`, `ExecuteReferenceNavigationInclude`, `ExecuteCollectionNavigationInclude`, `ExecuteSkipNavigationInclude`, `ScanOneToMany`)
   - Utility helpers (getter/setter compilation, tracking, debug diagnostics)

3. **It blocks M3 cleanly.** The bootstrap plan says *"Includes/relationships work via EF conventions and provider execution"*. Right now the include engine is embedded inside the compiling visitor — it can't be tested, evolved, or replaced independently.

4. **No behavior changes needed.** This is a pure structural refactor — the same tests pass, the same code runs, but each piece becomes independently testable and maintainable.

### Suggested extraction targets

| New class | Responsibility | Approximate lines |
|-----------|---------------|-------------------|
| `Db2RowPredicateCompiler` | `TryCompileRowHandlePredicate`, `TryTranslateConjunct`, `TryTranslateFieldAccess`, `TryGetVirtualIdPrimaryKeyEqualityLookup`, `SplitAndAlso` | ~400 |
| `Db2TableEnumerator` | `Table()` method, `ValueBufferReadPlan`/cache, join execution (`BuildLookup`, `JoinPlan`, key resolution) | ~600 |
| `Db2IncludeExecutor` | `ExecuteIncludes` + all include methods, `ScanOneToMany`, `MaterializeByIdsUntyped`, getter/setter/list-factory compilation, `TrackIfNeeded` | ~700 |
| Keep in visitor | `VisitShapedQuery`, `VisitExtension`, `Query<TResult>`, shaper compilation, limit rewriting, cardinality handling | ~500 |

### Approach

| Step | What |
|------|------|
| 1 | **Extract `Db2RowPredicateCompiler`** as an `internal static` class. Move all predicate analysis + compilation. `Table()` calls `Db2RowPredicateCompiler.TryCompile(...)`. |
| 2 | **Extract `Db2TableEnumerator`** as an `internal static` class. Move `Table()` and all `ValueBufferReadPlan` infrastructure. The compiling visitor calls `Db2TableEnumerator.Enumerate(...)` to get `IEnumerable<ValueBuffer>`. |
| 3 | **Extract `Db2IncludeExecutor`** as an `internal` class (or static). Move all include execution and utility helpers. `Query<TResult>` calls `Db2IncludeExecutor.Execute(...)`. |
| 4 | **Update tests** — no new tests needed, but verify the existing 195 still pass after each extraction step. |

### Acceptance criteria

- `MimironDb2ShapedQueryCompilingExpressionVisitor` is ≤600 lines.
- Each extracted class has a single, clear responsibility.
- All 195 tests continue to pass.
- No public API changes.

### What this unlocks afterward

- **M4 (expand translation coverage)**: `OrderBy`, `Skip`, `Distinct`, `LastOrDefault` can be added cleanly — `OrderBy`/`Skip` go into the `Db2QueryExpression` IR and `Db2TableEnumerator`, not a 2,300-line file.
- **Async query execution**: the skipped async test can be addressed by adding an async path in the table enumerator without touching the visitor.
- **Independent include improvements**: the include executor can be evolved toward EF-native patterns (M3) without risk to the core query path.
- **Per-component unit testing**: each extracted class can be covered with focused tests using the existing `TestDb2File` infrastructure.