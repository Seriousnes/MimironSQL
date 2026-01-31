# db2-query-engine-roadmap.md

This document describes the remaining work to reach the project’s end-goal: a fast, read-only DB2/WDC5 query engine with an EF-like LINQ surface where navigation access is supported in predicates/projections.

Supporting docs:

- DB2/WDC5 format notes: `./db2-format.md`
- Query engine notes (translation/execution): `./query-engine-notes.md`
- Architecture overview (if/when maintained): `./architecture.md`
- **Phase 6 design (collection navigations, multi-hop, SQL-text layer):** `./phase-6-design.md`

## Target User Experience

The query surface should support:

- Single-table queries: `Where`, `Select`, `Take`, `Any`, `Count`, `FirstOrDefault`, `Single`
- Navigation eager loading: `Include(e => e.Nav)`
- Navigation usage in predicates/projections, e.g. `context.Spell.Where(s => s.SpellName.Name_lang.Contains("Fire"))`

Navigation predicates/projections imply internal join semantics, but we do not need to expose `Join(...)` as a public API.

## Clarified Acceptance Criteria (Jan 2026)

- `Db2Model` stores schema-driven metadata per table (PK/FK as derived from `.dbd` + WDC5 layout) plus any `OnModelCreating` overrides.
- Entities without an `Id` member must explicitly configure a key in `OnModelCreating` (fail fast during model build).
- Schema hints are used to auto-configure FK-based navigations into the model; the model remains the single source of truth at query time.
- Phase 3 includes navigation access in both `Where(...)` and `Select(...)` (1-hop reference navigations).

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
    - primary key metadata (including required key member mapping)
    - FK/navigation/relationship metadata (schema-derived + overrides)
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
    - configuring primary key (required when the entity has no `Id` member)
    - configuring reference navigations:
        - FK-based navigations (schema-backed via auto-configuration, overridable)
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
- navigation access in both `Where(...)` and `Select(...)`

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

**Status:** Design/Planning (blocked by Phases 3–5)

See detailed design document: `./phase-6-design.md`

High-level scope:
- Collection navigations (1-to-many): `Include(x => x.Collection)`, `Any/All/Count` predicates
- Nested navigations (multi-hop): chained navigation access (e.g., `x.Nav1.Nav2.Field`)
- SQL-text layer (optional): text-based query interface if required for tooling/compatibility

Implementation order:
1. Collection navigations (highest value)
2. Multi-hop navigations (2-hop → 3+ hops)
3. SQL-text layer (only if justified by demand)

Prerequisites:
- All Phases 1–5 must be complete and stable
- Issues #8, #9, #10 must be resolved

## Concrete Implementation Breakdown (Jan 2026)

This section translates phases 4–5 into branch/PR-sized work items with commit-sized checkpoints.

Guiding constraints:

- Tests must use existing on-disk fixture DB2 data (no synthetic DB2 bytes).
- Preserve the “minimal decoding” philosophy: decode only fields required by the plan.

### PR 1 — “Required Columns per Source” (Phase 5 prerequisite)

Branch: `feature/phase-5-required-columns`

Commits:

1. Introduce requirement primitives used by execution/planning
    - Add small internal types to represent “required fields” per source (root/related): scalars, strings, join keys.
    - Keep them schema-driven (by `Db2TableSchema` field name + column index), with `MemberInfo` mapping only at translation boundaries.
2. Extend the navigation plan to carry per-source requirements
    - Navigation translation should return: join linkage + required columns for root and related source.
    - Ensure navigation predicates mark required columns on the related source, and join keys on both sources.
3. Wire requirements into pruning decisions
    - Refine pruning eligibility so projections that require navigation or extra fields don’t accidentally “prune” into a path that can’t satisfy required columns.
4. Add/extend fixture-backed tests proving requirements affect decoding
    - Prefer tests that validate observable behavior (e.g., constructor-count / materialization counters) using existing fixtures.

Acceptance criteria:

- A multi-source plan explicitly records which fields are required on the root and on the related source.
- Existing pruning behavior remains correct, and navigation projections/predicates no longer “accidentally” force full entity decoding.

### PR 2 — “Efficient Navigation Projections” (Phase 5)

Branch: `feature/phase-5-batched-nav-projections`

Commits:

1. Add an internal batch navigation loader
    - Given a join linkage and a set of required related fields, build a key→row/field lookup for the related table.
    - Implement left-join semantics for projections: missing related row ⇒ projected value is `null`/default.
2. Add a projection path for `Select(x => x.Nav.SomeField)` without N+1
    - Collect distinct keys from root scan, batch load related lookup once, project from lookup.
    - Do not materialize the related entity when projecting a scalar/anonymous result.
3. Expand fixture-backed tests to assert both correctness and “non-N+1” behavior
    - Use real fixture DB2 data.
    - Prefer deterministic assertions (e.g., specific known IDs/values already used in tests) rather than heuristics.

Acceptance criteria:

- Navigation access in `Select(...)` no longer performs per-row `TryGetRowById`-style lookups.
- Projections that read related fields can avoid materializing full entities.

### PR 3 — “Batch Include + Broader Predicate Shapes” (Phase 4 + Phase 3+)

Branch: `feature/phase-4-batched-include-and-predicate-shapes`

Commits:

1. Batch `Include(...)` to avoid N+1
    - Reuse the batch navigation loader to populate navigations in one pass.
    - Preserve current semantics: missing related row ⇒ navigation is `null`.
2. Support additional navigation predicate shapes (start with AND)
    - Support `AND` combinations of multiple navigation string predicates on the same navigation by intersecting key sets.
    - Support mixing root predicates with navigation predicates by composing row predicates.
3. Optional follow-up: captured needles for string predicates
    - Support closed-over `string` variables (simple closures) in nav string predicates.
4. Fixture-backed tests for each new predicate shape
    - Use existing DB2 fixtures only.

Acceptance criteria:

- `Include(...)` is batched and does not do per-entity table lookups.
- Navigation predicates support at least: `AND` of two nav-string predicates on the same navigation, plus mixing with a root predicate.

