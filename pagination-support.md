## Implement `OrderBy` / `ThenBy` + `Skip` (Pagination Support)

### Why this is the highest-impact next step

With M0–M2.5 complete and the god-class decomposed, the codebase is well-structured for feature additions. The next milestone is **M4: Expand translation coverage**. Among the 25+ operators that still throw `NotSupportedException`, **OrderBy/ThenBy and Skip** are the most impactful to add because:

1. **Most commonly used after Where/Select/Include** — any real-world consumer of the provider will need to sort results
2. **Enables pagination** — `OrderBy` + `Skip` + `Take` is the standard EF Core pagination pattern, and `Take` already works
3. **Unlocks `LastOrDefault`** — can be rewritten as `OrderByDescending` + `Take(1)`, making it trivially implementable once ordering exists
4. **Clean vertical slice through 3 layers** — IR, translation, execution — without architectural risk
5. **No external dependencies** — pure in-process sorting over already-materialized rows; no changes to `IDb2File` or `IDb2Format` needed

### What currently exists (and doesn't)

| Layer | OrderBy/ThenBy | Skip |
|---|---|---|
| **IR (`Db2QueryExpression`)** | No `Orderings` list | No `Offset` property |
| **Translation visitor** | Throws `NotSupportedException` | Throws `NotSupportedException` |
| **Execution (`Db2ClientQueryExecutor`)** | No sorting applied | No offset applied |
| **Tests** | None | None |

### Approach

#### 1. Extend `Db2QueryExpression` IR (~15 lines)

Add to [Db2QueryExpression.cs](src/MimironSQL.EntityFrameworkCore/Query/Internal/Expressions/Db2QueryExpression.cs):

```csharp
// Ordering: list of (keySelector lambda, ascending flag) pairs
public List<(LambdaExpression KeySelector, bool Ascending)> Orderings { get; } = [];

// Skip/Offset
public Expression? Offset { get; private set; }

public void ApplyOrdering(LambdaExpression keySelector, bool ascending)
{
    Orderings.Add((keySelector, ascending));
}

public void ApplyOffset(Expression offset)
{
    // Similar to ApplyLimit — capture as expression for parameter support
    Offset = offset;
}
```

#### 2. Implement translation (~30 lines)

In [MimironDb2QueryableMethodTranslatingExpressionVisitor.cs](src/MimironSQL.EntityFrameworkCore/Query/Internal/MimironDb2QueryableMethodTranslatingExpressionVisitor.cs), replace the `NotSupportedException` stubs:

- **`TranslateOrderBy`**: Store `(keySelector, ascending)` on the `Db2QueryExpression`. If orderings already exist, clear and replace (EF Core semantics: `OrderBy` resets prior orderings).
- **`TranslateThenBy`**: Append `(keySelector, ascending)` to existing orderings.
- **`TranslateSkip`**: Store the offset expression on `Db2QueryExpression`, mirroring how `TranslateTake` stores the limit.

#### 3. Apply ordering and offset in execution (~40 lines)

In [Db2ClientQueryExecutor.cs](src/MimironSQL.EntityFrameworkCore/Query/Internal/Db2ClientQueryExecutor.cs), after collecting `results` and before applying includes:

- If `queryExpression.Orderings.Count > 0`, apply client-side `LINQ OrderBy/ThenBy` over the materialized result list using compiled key selectors.
- If offset > 0, apply `.Skip(offset)` before the limit/take.

This keeps the pattern consistent: the `Db2TableEnumerator` produces `ValueBuffer`s, and `Db2ClientQueryExecutor` applies post-materialization operations (predicates, ordering, skip, take, includes).

#### 4. Wire `RewriteOffsetExpression` in the compiling visitor (~15 lines)

In [MimironDb2ShapedQueryCompilingExpressionVisitor.cs](src/MimironSQL.EntityFrameworkCore/Query/Internal/MimironDb2ShapedQueryCompilingExpressionVisitor.cs), use the same `RewriteLimitExpression` approach to pass the offset as an `int` argument to `Query<TResult>()`, supporting both constant and parameterized skip values.

#### 5. Unlock `LastOrDefault` (~10 lines)

Once ordering exists, `TranslateLastOrDefault` can be implemented as: reverse the orderings (flip ascending flags), apply predicate if present, `Take(1)`, return `SingleOrDefault` cardinality.

#### 6. Tests

- **Unit tests**: OrderBy/ThenBy/Skip compile and translate correctly
- **Integration tests**: pagination pattern (`OrderBy` + `Skip` + `Take`), `LastOrDefault`, multi-key ordering (`OrderBy` + `ThenBy`), descending order

### Acceptance Criteria

- `ctx.Set<Spell>().OrderBy(s => s.Name).Take(10).ToList()` returns 10 items in sorted order
- `ctx.Set<Spell>().OrderBy(s => s.Id).Skip(5).Take(10).ToList()` skips the first 5, returns next 10
- `ctx.Set<Spell>().OrderByDescending(s => s.Id).ThenBy(s => s.Name).ToList()` applies compound sort
- `LastOrDefault` works via ordering reversal
- All 195 existing tests still pass; new tests added for each scenario
- Parameterized skip values (captured variables) work correctly

### What it unlocks next

With ordering + skip + take working, the provider covers the **80% usage case** for read-only data browsing. After this, the natural next steps would be:

- **Aggregates** (`Min`/`Max`/`Sum`/`Average`) — extend the `Db2TerminalOperator` enum, same pattern as `Count`
- **String predicate pushdown** — promote `StartsWith`/`EndsWith`/`Contains(string)` from client-side to row-level for performance
- **Async query execution** — unblock the 1 skipped test

### Risk: Low

- No architectural changes — only additive IR fields, translation, and execution logic
- Client-side sorting is correct by construction (LINQ-to-Objects)
- The decomposed codebase makes it clear exactly where each piece goes
- No changes to `IDb2File`, `IDb2Format`, or the schema layer