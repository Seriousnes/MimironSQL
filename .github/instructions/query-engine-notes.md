# query-engine-notes.md

This document collects execution and translation notes for the query engine.

## Core principle

Preserve the “minimal decoding” philosophy:

- Decode only columns required by predicates/projections.
- Avoid materializing full entities when the caller’s projection does not require them.

## Relationship source of truth

- Relationships are resolved from `Db2Model` (context-scoped), not from WDC5 parsing.
- Schema (`.dbd`) provides default conventions (field names/types/referenced table name when present).
- `Db2ModelBuilder` provides overrides and additional relationships not present in DB2/DBD (e.g., shared-primary-key 1:1).

## Navigation-aware expression translation

Target: support navigation access inside `Where` and `Select` without exposing `Join(...)` publicly.

Initial scope:

- Reference navigations only.
- One-hop navigation member access (e.g., `s.SpellName.Name_lang`).

Translation outcome:

- A multi-source plan (root table + one related table)
- Join linkage:
  - FK → PK, or
  - shared PK → shared PK (implicit 1:1)
- Per-source required columns for predicates/projections.

## Execution strategies

### Semi-join for navigation predicates

When filtering root rows based on a predicate on a related table:

- Evaluate predicate against related table first.
- Collect matching keys (typically `HashSet<int>`).
- Scan root table and filter by membership.

This avoids per-row join work and pairs well with dense string-table scanning.

### Hash-join-style lookup for `Include` and projections

For `Include(...)` and projections that read related columns:

- Collect required keys from root.
- Load related rows in batch into a lookup keyed by PK.
- Populate navigation values and/or drive projections from lookup results.

## Null / missing-row semantics

Define and keep consistent:

- `Include(...)` should behave like a left join: missing related row => navigation is `null`.
- Navigation predicates/projections must define behavior when navigation is missing:
  - Either require explicit null checks in the expression, or
  - Define a consistent implicit behavior (e.g., treat missing as false for predicates).

## Projection support (navigations)

Support projections that read related columns:

- Anonymous/scalar projections should avoid full entity materialization when possible.
- Request minimal columns across both sources.
- Ensure stable semantics for missing navigations.
