# Phase 6 Design — Collection Navigations, Multi-Hop Navigations, and SQL-Text Layer

**Status:** Design/Planning (blocked by Phases 3–5 completion)

**Dependencies:**
- Blocked by: #8 (Phase 3 — Navigation-aware translation)
- Blocked by: #9 (Phase 4 — Multi-source execution semantics)
- Blocked by: #10 (Phase 5 — Navigation projections)

**Prerequisites:**
Before beginning Phase 6 implementation, Phases 1–5 must be complete and stable, with all open issues resolved.

---

## Overview

Phase 6 extends the query engine with advanced navigation capabilities that build upon the reference navigation support established in Phases 1–5. This phase is optional and should be undertaken only after the foundation is proven stable in production-like scenarios.

### Scope

1. **Collection Navigations (1-to-many)** — Support navigating from a parent entity to a collection of related child entities
2. **Multi-Hop Navigations (nested)** — Support chained navigation access (e.g., `x.Nav1.Nav2.Field`)
3. **SQL-Text Layer (if needed)** — Optional text-based query interface for compatibility or tooling integration

---

## 1. Collection Navigations (1-to-many)

### Goal

Enable queries that navigate from a principal entity to multiple related dependent entities, with efficient batch loading and intuitive LINQ semantics.

### User Experience

```csharp
// Include a collection navigation
var spells = context.Spell
    .Include(s => s.SpellEffects)
    .Where(s => s.Id > 1000)
    .ToList();

// Filter by collection navigation predicate
var spellsWithScalingEffect = context.Spell
    .Where(s => s.SpellEffects.Any(e => e.Effect == 53)) // EffectScaling
    .ToList();

// Project from collection navigations
var spellEffectCounts = context.Spell
    .Select(s => new { s.Id, EffectCount = s.SpellEffects.Count() })
    .ToList();
```

### Design Considerations

#### Model Configuration

Collection navigations must be explicitly configured in `Db2ModelBuilder`:

```csharp
protected override void OnModelCreating(Db2ModelBuilder modelBuilder)
{
    modelBuilder.Entity<Spell>()
        .HasMany(s => s.SpellEffects)
        .WithOne()
        .HasForeignKey(e => e.SpellID);
}
```

Key metadata required:
- Principal entity type and key member
- Dependent entity type and foreign key member
- Collection property on principal entity (must be `ICollection<T>` or similar)
- Inverse navigation (if present)

#### Execution Strategies

**For `Include(...)`:**
- Collect distinct principal keys from the root query result
- Batch load all related dependents in a single pass using an indexed scan: `WHERE dependentFK IN (principalKeys)`
- Group results by foreign key value
- Populate collection properties via reflection or compiled setters

**For collection predicates (`Any`, `All`, `Count`):**
- `Any(pred)` translates to a semi-join: evaluate predicate on dependent table, collect matching FKs, filter principal by key membership
- `All(pred)` requires inverse logic: find principals that have NO dependents OR ALL dependents satisfy the predicate
- `Count()` in predicates may require a separate aggregation pass

**For collection projections:**
- Similar to `Include`, but may only materialize required fields from dependents
- `Count()` projections can use a GROUP BY aggregation on the dependent table instead of materializing entities

#### Null/Empty Collection Semantics

- A principal with no related dependents returns an **empty collection**, not `null`
- This aligns with EF Core semantics and is consistent with SQL LEFT JOIN behavior

#### Performance Notes

- Collection loading is inherently more expensive than reference navigation (1-to-1 or many-to-1)
- Use batched lookups to avoid N+1 queries
- For very large collections, consider limiting results or using pagination at the query level
- `Any` and `Count` predicates should prefer indexed scans on the dependent table rather than materializing full collections

---

## 2. Multi-Hop Navigations (Nested/Chained)

### Goal

Support chained navigation member access across multiple relationships, enabling natural expression of complex queries without explicit joins.

### User Experience

```csharp
// 2-hop: Spell -> SpellName -> Localized text
var fireSpells = context.Spell
    .Where(s => s.SpellName.Name_lang.Contains("Fire"))
    .ToList();

// 3-hop: through multiple tables
var results = context.SomeTable
    .Where(x => x.Nav1.Nav2.Nav3.SomeField > 100)
    .ToList();

// Mixed: reference + collection
var spellsWithScalingOnAuraEffect = context.Spell
    .Where(s => s.AuraOptions.SpellAuraEffects.Any(e => e.EffectIndex == 0))
    .ToList();
```

### Design Considerations

#### Translation Strategy

Multi-hop navigation access must be recursively analyzed during expression translation:

1. **Parse the navigation chain:** Identify each navigation step from root to leaf
2. **Resolve metadata for each hop:** Use `Db2Model` to look up navigation metadata at each step
3. **Build a multi-source plan:** Include all intermediate tables as sources in the query plan
4. **Determine join linkage:** Chain joins from root → intermediate1 → intermediate2 → ... → leaf
5. **Propagate requirements:** Required columns for predicates/projections must be marked on the correct source

Example plan for `x.Nav1.Nav2.Field`:
```
Root Table (x)
  JOIN Nav1 Table ON Root.Nav1_FK = Nav1.PK
  JOIN Nav2 Table ON Nav1.Nav2_FK = Nav2.PK
  WHERE Nav2.Field = <value>
```

#### Execution Considerations

**For predicates:**
- Use semi-join optimization when possible: evaluate innermost predicate first, propagate matching keys outward
- For 2-hop: `Root → A → B WHERE B.Field = X`:
  1. Scan B for matching rows, collect A_PKs
  2. Scan A for rows matching A_PKs, collect Root_FKs
  3. Scan Root for rows matching Root_FKs

**For projections:**
- Batch load each intermediate source as needed
- Build nested lookups: `Root_FK → A_Row → B_Row → Field`
- Populate projections by traversing the lookup chain per root row

**For `Include(...)`:**
- Each hop requires a separate batch load or nested join
- Intermediate entities may need to be materialized if the navigation chain is exposed in the result

#### Complexity and Limitations

- Deep navigation chains (4+ hops) may have significant performance costs
- Consider limiting initial support to 2–3 hops and expanding based on real-world use cases
- Circular navigation references are not supported (detect and fail during model build)
- Each hop increases plan complexity; document the performance implications clearly

#### Null Propagation

Multi-hop navigations must handle null navigations at intermediate steps:

- **Option 1 (strict):** Treat `x.Nav1.Nav2.Field` as requiring both Nav1 and Nav2 to be non-null; filter out rows where any intermediate navigation is missing
- **Option 2 (null-propagating):** Use left-join semantics at each hop; missing intermediate navigations result in `null` for the final value
- **Recommendation:** Use Option 1 (strict) for predicates to avoid ambiguity, Option 2 (null-propagating) for projections to align with EF Core semantics

---

## 3. SQL-Text Layer (Optional)

### Goal

Provide a text-based SQL-like query interface as an alternative or complement to LINQ for scenarios where:
- Tooling integration requires SQL compatibility
- Dynamic query construction is easier with string-based syntax
- Users prefer SQL familiarity over LINQ

### User Experience

```csharp
// Execute raw SQL-like queries
var results = context.ExecuteSql<Spell>(
    "SELECT * FROM Spell WHERE Name_lang LIKE '%Fire%' LIMIT 10");

// Support parameters
var results = context.ExecuteSql<Spell>(
    "SELECT * FROM Spell WHERE Id > @minId AND SpellName.Name_lang LIKE @pattern",
    new { minId = 1000, pattern = "%Frost%" });
```

### Design Considerations

#### SQL Dialect

Define a minimal SQL-like dialect that maps to the existing query engine capabilities:

- `SELECT <columns> FROM <table>`
- `WHERE <predicate>`
- `ORDER BY <columns>`
- `LIMIT <count>`
- Navigation access in predicates/projections using dot notation (e.g., `SpellName.Name_lang`)
- Parameter binding using `@paramName` syntax

**Not supported:**
- Explicit `JOIN` syntax (navigations are used instead)
- `GROUP BY`, `HAVING` (not currently supported by the engine)
- Subqueries
- `UPDATE`, `INSERT`, `DELETE` (read-only engine)

#### Implementation Approach

1. **Lexer/Parser:** Use a simple recursive-descent parser or a parser generator (e.g., Pidgin, Sprache) to parse SQL-text into an AST
2. **AST → Expression Translation:** Convert the parsed AST into LINQ expression trees that can be consumed by the existing query pipeline
3. **Parameter Binding:** Map SQL parameters to closed-over constants in the expression tree
4. **Validation:** Ensure the query text maps to supported operations; fail fast with clear errors for unsupported syntax

#### Alternative: Leverage Existing Tools

Instead of building a custom SQL parser, consider:
- **Dapper-style raw queries:** Allow raw SQL-like text but defer to the existing LINQ query compiler internally
- **OData/GraphQL:** If the goal is tooling integration, consider OData query syntax or GraphQL instead of SQL
- **Skip entirely:** If LINQ suffices for all use cases, defer SQL-text layer indefinitely

### When to Implement

The SQL-text layer should be the **last** part of Phase 6 to implement, and only if:
- There is clear user demand or tooling requirements
- LINQ query capabilities are proven stable and feature-complete
- Resources are available for parser development and maintenance

---

## Implementation Sequence

**Recommendation:** Implement Phase 6 features in the following order:

1. **Collection Navigations (1-to-many)**
   - Highest value for common use cases
   - Builds naturally on reference navigation foundation from Phases 1–5
   - Tests can reuse existing fixture data (e.g., `Spell` → `SpellEffect`)

2. **Multi-Hop Navigations (2-hop initially)**
   - Moderate complexity increase
   - Validate execution strategies with 2-hop before expanding to deeper chains
   - Common use case: `Spell.SpellName.Name_lang`

3. **Multi-Hop Extensions (3+ hops, mixed reference/collection)**
   - Expand to deeper chains once 2-hop is stable
   - Support mixed navigation types (reference + collection in a chain)

4. **SQL-Text Layer (if needed)**
   - Implement only if justified by user demand or integration requirements
   - Can be deferred indefinitely if LINQ suffices

---

## Testing Strategy

All Phase 6 features must be validated with:

- **Fixture-backed tests** using existing on-disk DB2 data (no synthetic data)
- **Performance tests** proving batched execution (no N+1 queries)
- **Correctness tests** asserting both positive cases and edge cases (empty collections, null navigations, etc.)
- **Error tests** proving clear error messages for misconfiguration or unsupported operations

Example test scenarios:
- `Include(s => s.SpellEffects)` loads all related effects in a single batch
- `Where(s => s.SpellEffects.Any(e => e.Effect == 53))` uses semi-join execution
- `Select(s => s.SpellName.Name_lang)` performs 2-hop batch lookup without N+1
- Querying a principal with no dependents returns an empty collection, not null
- Multi-hop with a missing intermediate navigation returns null (or filters out) as documented

---

## Acceptance Criteria (Summary)

### Collection Navigations

- [ ] `HasMany(...).WithOne(...)` configuration API is implemented
- [ ] `Include(x => x.CollectionNav)` batches dependent loading (no N+1)
- [ ] `Any`, `All`, `Count` predicates on collections execute via semi-join/aggregation (no N+1)
- [ ] Empty collections are returned as empty (not null)
- [ ] Tests prove correctness and performance using fixture data

### Multi-Hop Navigations

- [ ] 2-hop navigation access in predicates/projections is supported (e.g., `x.Nav1.Nav2.Field`)
- [ ] 3+ hop chains are supported (documented performance characteristics)
- [ ] Null propagation semantics are defined and tested
- [ ] Mixed reference/collection chains are supported (if scope includes)
- [ ] Tests prove correctness and batched execution using fixture data

### SQL-Text Layer

- [ ] (Optional) SQL-like text query interface is implemented with documented dialect
- [ ] (Optional) Parameter binding is supported
- [ ] (Optional) Error messages for unsupported syntax are clear
- [ ] (Optional) Tests prove correct translation to LINQ and execution

---

## Open Questions (To Be Resolved During Implementation)

1. **Collection navigation cardinality limits:** Should there be a configurable limit on collection sizes to prevent memory issues?
2. **Inverse navigations:** Should `WithOne(x => x.ParentNav)` configure bidirectional navigations, or keep them unidirectional?
3. **Null semantics for multi-hop:** Strict (filter out) vs. null-propagating (left join) — choose one or make configurable?
4. **SQL-text dialect specifics:** If implemented, should the SQL dialect match T-SQL, PostgreSQL, or a custom minimal syntax?
5. **Performance tuning:** What heuristics should guide when to use semi-join vs. hash-join for collection predicates?

---

## Related Documentation

- Main implementation plan: `./implementation-plan.md`
- Architecture overview: `./architecture.md`
- Query engine execution notes: `./query-engine-notes.md`
- DB2/WDC5 format notes: `./db2-format.md`

---

**Note:** This document will be refined and expanded as Phases 3–5 stabilize and Phase 6 implementation begins.
