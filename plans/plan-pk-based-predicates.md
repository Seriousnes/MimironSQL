# Plan: Unify Primary-Key Based Predicate Execution

## Problem Statement

Two different queries — a Map query (full-scan path) and a Spell query (key-lookup path) — both filter by primary key, yet they follow entirely different execution paths:

### Map query (full-scan path)
```csharp
context.Map.Where(x => x.Id > 0).Take(10).ToListAsync()
```
- `FileSystemDb2StreamProvider.OpenDb2Stream()` is called once
- `IDb2Format.OpenFile()` → `new Wdc5File(stream)` loads the **entire file** into memory (all record data, string tables, index data, copy tables)
- The query enumerates all rows, materializing every entity, then filters in-memory

### Spell query (key-lookup path)
```csharp
context.Set<SpellEntity>().SingleOrDefault(x => x.Id == 454009)
```
- Hits the key-lookup fast path in `MimironDb2QueryExecutor.ExecuteTyped`
- `TryGetKeyLookupRequest` pattern-matches the expression and extracts the ID
- `MimironDb2Store.TryMaterializeById` opens a stream, parses `Wdc5KeyLookupMetadata` (header + index only, no record data), resolves the row handle, opens a **second** stream, creates `Wdc5KeyLookupRowFile` to read just that one row

### Why this is a problem
1. The Map query with `Where(x => x.Id > 0)` should ideally not require loading the entire file when only 10 rows are needed; however, `Id > 0` is a range predicate (not equality), so key-lookup is correctly not triggered. This is acceptable for now.
2. However, if the Map query were `context.Map.SingleOrDefault(x => x.Id == 42)`, it would currently still go through the full-scan path depending on predicate pattern — the key-lookup eligibility check (`TryGetKeyLookupRequest`) has strict requirements.
3. More broadly, the key-lookup fast path is only available for **exact equality on a single PK value**. Any other PK-based predicate (ranges, `IN` sets, etc.) forces a full table load.

## Current Key-Lookup Eligibility (in `TryGetKeyLookupRequest`)

A query qualifies for key-lookup only if ALL of:
- Row type is `RowHandle`
- Entity has a discoverable primary key
- Pipeline has exactly **one** `Where` with an equality on the PK (e.g., `x => x.Id == 42`)
- No `Include`, `Select`, or `Skip` operations
- If `Take` is present, it must be `Take(1)`

## Proposed Changes

### Step 1: Extend key-lookup to support multiple IDs
Allow `TryGetKeyLookupRequest` to extract a **set** of IDs from predicates like:
- `Where(x => x.Id == 42)` → single ID (existing)
- `Where(x => new[] { 1, 2, 3 }.Contains(x.Id))` → multiple IDs
- `Where(x => x.Id == 1 || x.Id == 2)` → multiple IDs via OR

Change `KeyLookupRequest` from holding a single `int Id` to holding an `int[]` or `IReadOnlyList<int>` of IDs.

### Step 2: Allow key-lookup with `Include` operations
Currently, `Include` immediately disqualifies key-lookup. Since `TryMaterializeById` already returns a fully materialized entity, navigations could be resolved after the primary entity is found via separate key-lookups on the related tables.

This is a larger change and may be deferred — document it as a future improvement.

### Step 3: Allow key-lookup with `Select` projections
If the query is `context.Set<Spell>().Where(x => x.Id == 42).Select(x => x.Name)`, we should still use key-lookup to find the row, then apply the projection after materialization.

### Step 4: Ensure the full-scan path uses _idIndex when possible
When the full-scan path (`Db2QueryProvider`) encounters a WHERE predicate that can be resolved entirely via the `_idIndex`, it should do so rather than enumerating all rows. This means:
- After the `Wdc5File` is opened, check if the compiled `Where` predicate is strictly PK-equality-based
- If so, use `TryGetRowHandle` to resolve the matching rows directly

### Step 5: Eliminate redundant stream opens
Currently, `TryMaterializeById` opens one stream for metadata parsing, caches the metadata, then opens a **second** stream for the row data. The `Wdc5File` approach reads everything from a single stream. The key-lookup path should either:
- Accept a pre-opened `IDb2File` (once DRY violations are resolved and `Wdc5File` supports lazy row reading), or
- At minimum, reuse the same stream for both metadata and row reading

## Dependencies
- **Plan: DRY Violations** — Once `Wdc5File` is consolidated, key-lookup functionality should be built into `Wdc5File` itself (lazy id-index building + on-demand row reading without full record blob allocation).
- **Plan: Tight Coupling Removal** — The key-lookup path must work through `IDb2Format`/`IDb2File` abstractions, not direct `Wdc5*` references.

## Acceptance Criteria
- [ ] Equality PK predicates on any entity use the key-lookup fast path regardless of which `IDb2StreamProvider` is used
- [ ] Key-lookup supports extracting multiple IDs from `Contains` / OR expressions
- [ ] No redundant stream opens for a single key-lookup query
- [ ] Full-scan path with PK equality predicates delegates to `_idIndex` where possible
- [ ] All existing integration tests pass unchanged
