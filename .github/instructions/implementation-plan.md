# db2-query-engine-roadmap.md

This document describes the remaining work to reach the project’s end-goal: a fast, read-only DB2/WDC5 query engine with an EF-like LINQ surface where navigation access is supported in predicates/projections.

Supporting docs:

- DB2/WDC5 format notes: `./db2-format.md`
- Query engine notes (translation/execution): `./query-engine-notes.md`
- Architecture overview (if/when maintained): `./architecture.md`

## Target User Experience

The query surface should support:

- Single-table queries: `Where`, `Select`, `Take`, `Any`, `Count`, `FirstOrDefault`, `Single`
- Navigation eager loading: `Include(e => e.Nav)`
- Navigation usage in predicates/projections, e.g. `context.Spell.Where(s => s.SpellName.Name_lang.Contains("Fire"))`

Navigation predicates/projections imply internal join semantics, but we do not need to expose `Join(...)` as a public API.

## Approach (EF-like, not EF Core)

Goal: maximize API familiarity with EF Core patterns while keeping a purpose-built execution engine optimized for WDC5.

Non-goals:

- Implementing an EF Core database provider (provider services, EF query pipeline integration, change tracking, migrations).
- Matching every EF Core semantic edge case; prefer clear, documented semantics that preserve performance and “minimal decoding”.

API familiarity targets:

- `Db2Context.OnModelCreating(Db2ModelBuilder modelBuilder)` configuration hook.
- Optional `IDb2EntityTypeConfiguration<T>` (EF-like `IEntityTypeConfiguration<T>`) and basic apply helpers (only if needed).
- `Include(e => e.Nav)` eager loading that batches lookups (avoid N+1).
- Navigation access in `Where`/`Select` that translates to internal multi-source plans (no public `Join`).

## Remaining Work (Phased)

### 1) Context Model Foundation (`Db2Model`)

Goal: move “what entities/relationships exist” into a context-scoped model so it becomes the single source of truth for navigation resolution.

Deliverables:

- `Db2Model` describing:
    - entity ↔ table name mapping
    - primary key metadata
    - navigation/relationship metadata
    - support for relationships not present in DB2/DBD (e.g., shared-primary-key 1:1 like `Spell` ↔ `SpellName`)
- `Db2Context` builds/caches its `Db2Model` once and exposes it to query translation/execution.

Notes:

- `Db2Model` must be the single source of truth for relationships/navigations (schema provides defaults; the model can override/extend).
- Keep WDC5 parsing and row decoding free of relationship knowledge.

### 2) Configuration Surface (`Db2ModelBuilder`)

Goal: an EF-like configuration hook to override/add metadata without encoding app knowledge into the binary/schema layers.

Deliverables:

- `Db2ModelBuilder` with a minimal fluent API for:
    - configuring table name for an entity
    - configuring primary key
    - configuring reference navigations:
        - FK-based navigations (existing schema-backed case)
        - shared-primary-key navigations (implicit relationship case)
- `Db2Context.OnModelCreating(Db2ModelBuilder modelBuilder)` as the primary extension point.

Deferred (only if needed): assembly scanning / `ApplyConfigurationsFromAssembly`.

Notes:

- Prefer a minimal, strongly-typed fluent surface that maps 1:1 to what the engine needs (keys + reference navigations + shared-PK 1:1).
- Avoid a conventions engine unless a clear need emerges.

### 3) Navigation-aware Expression Translation

Goal: translate navigation member access inside `Where`/`Select` into a multi-source query plan.

Scope (initial):

- 1-hop reference navigation only (e.g., `s.SpellName.Name_lang`, not nested chains)
- translate navigation access using `Db2Model` relationships
- initial string predicate support should include `==`, `Contains`, `StartsWith`, and `EndsWith` against related-table string fields

Deliverables:

- Expression analysis that detects navigation member access and produces a query plan with:
    - root source (table)
    - one related source
    - join linkage (FK→PK or shared-PK)
    - predicate/projection requirements per source

Notes:

- This is “join semantics without a public `Join` API”: navigation access drives the plan.
- Start with 1-hop navigations; expand only after execution + projection support are stable.

### 4) Multi-source Execution Strategies

Goal: execute the multi-source plan efficiently while preserving the “minimal decoding” philosophy.

Deliverables:

- For navigation predicates:
    - prefer semi-join execution when possible (evaluate predicate on related table first → get matching keys → filter root)
    - specifically for string predicates (`==`, `Contains`, `StartsWith`, `EndsWith`) on the related table, reuse dense string-block scanning where available
- For `Include(...)`:
    - batch load related rows (avoid N+1) via key collection + lookup
- Define null/missing-row semantics consistently (e.g., left-join behavior for `Include`, explicit null checks in predicates).

Notes:

- Favor semi-join when a predicate only depends on the related table.
- Use batched lookups for `Include` and navigation projections (hash-join-style under the hood).

### 5) Projection Support with Navigations

Goal: allow `Select(...)` projections that read values from navigations.

Deliverables:

- Projection planning that requests the minimal set of columns across both sources
- Efficient execution path for:
    - scalar projections
    - anonymous-type projections
    - entity materialization + navigation materialization (when requested)

Notes:

- Projections that read navigation values require related-row lookup; treat this as an internal join concern.
- Preserve single-table column pruning concepts per source where possible.

### 6) Optional / Later

- Collection navigations (1-to-many)
- Nested navigations (multi-hop)
- SQL-text layer (only if required)
