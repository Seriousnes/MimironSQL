## Native `Where` Pushdown

### Rational

1. **Biggest efficiency gap today.** Every query — including `Find`, `First`, `Any`, `Count` — currently does a full table scan, materializes every row into a `ValueBuffer`, shapes it into an entity, and *then* applies the predicate in `Query<TResult>()`. For a 100k-row table with a `Where(x => x.Id == 42)`, that's 99,999 wasted materializations.

2. **Defines what "database provider" means.** The distinction between an EF Core provider and a LINQ-to-Objects wrapper is whether filters run at the data source. This is the architectural boundary you're crossing.

3. **Enables `Find` as a special case.** Once the `Table()` method can evaluate a predicate against row handles, `Find(key)` becomes "`Where(pk == key).Take(1)`" with a fast path via `TryGetRowById` when the key maps to the virtual ID — no separate `IEntityFinder` wiring needed.

4. **All plumbing is already in place.** Predicates are already captured in `Db2QueryExpression.Predicates`. The `ValueBufferReadPlan` already knows how to compile per-field `ReadField<T>` readers. The change is moving predicate evaluation from [Query<TResult>()](src/MimironSQL.EntityFrameworkCore/Db2/Query/MimironDb2ShapedQueryCompilingExpressionVisitor.cs#L458) into [Table()](src/MimironSQL.EntityFrameworkCore/Db2/Query/MimironDb2ShapedQueryCompilingExpressionVisitor.cs#L1728), operating on individual fields rather than shaped entities.

### Suggested approach

| Step | What | Detail |
|------|------|--------|
| 1 | **Predicate analyzer** | Walk each `LambdaExpression` in `Db2QueryExpression.Predicates`. Classify each conjunct as *translatable* (simple field comparison, null check, boolean composition) or *client-side*. Split `&&` chains; keep `\|\|` whole per safe-pushdown rules (plan §4.1). |
| 2 | **DB2 row-handle predicate** | For translatable conjuncts, compile a `Func<IDb2File, RowHandle, bool>` that calls `ReadField<T>` on only the referenced columns. This runs inside the `Table()` enumerator, skipping non-matching rows before `ValueBuffer` construction. |
| 3 | **Client-side remainder** | Any non-translatable predicates stay in `Query<TResult>()` exactly as today — no behavior change for unsupported shapes. |
| 4 | **Virtual-ID fast path** | When the only predicate is `pk == constant` and the PK maps to `Db2VirtualFieldIndex.Id`, replace the full scan with `TryGetRowById` — instant `Find` semantics. |

### MVP predicate shapes (from plan §6.2)

- `==`, `!=`, `<`, `<=`, `>`, `>=` against constants/parameters
- Null checks (`== null`, `!= null`)
- `&&`, `||`, `!` with the safe partial-pushdown rules
- `Contains`/IN against in-memory sets (with the `TryGetRowById` optimization for virtual-ID PK)

### Acceptance criteria

- `Where` filters skip rows before `ValueBuffer` construction for all MVP shapes.
- `Find(key)` on a virtual-ID PK reads exactly one row.
- Unsupported predicates still execute correctly client-side.
- All 191 tests continue to pass.

### What this unlocks afterward

- **M2.5 complete** per the bootstrap plan.
- `Find` / `First` / `Single` / `Any` / `Count` all benefit automatically.
- Opens the door to **M3** (moving includes toward EF-native patterns) and **M4** (expanding LINQ coverage with `OrderBy`, `Skip`, `Distinct`) from a position of having a real filter-at-source execution model.