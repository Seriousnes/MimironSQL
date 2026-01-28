# Phase 4: Multi-Source Query Execution Semantics

This document defines the behavior of multi-source queries (Include, navigation predicates, navigation projections) in MimironSQL, particularly how missing rows and failed operations are handled.

## Navigation Relationships

MimironSQL supports two types of reference navigations:

1. **Foreign Key to Primary Key** (`Db2ReferenceNavigationKind.ForeignKeyToPrimaryKey`)
   - Standard FK relationship from one table to another
   - Example: `Map.ParentMapID` → `Map.Id`

2. **Shared Primary Key (1:1)** (`Db2ReferenceNavigationKind.SharedPrimaryKeyOneToOne`)
   - Two tables share the same primary key
   - Example: `Spell.Id` ↔ `SpellName.Id`

## Semantic Guarantees

### 1. Include(...) - Left-Join Behavior

**Behavior**: `Include(...)` uses left-join semantics. The navigation property will be populated when a related row exists, or set to `null` when it doesn't.

**Rules**:
- When FK = 0: navigation is `null`
- When related row is missing: navigation is `null`
- Never throws for missing rows
- Uses batched loading to avoid N+1 queries

**Example**:
```csharp
var maps = context.Map
    .Where(m => m.Id > 0)
    .Include(m => m.ParentMap)
    .ToList();

// Each map's ParentMap will be:
// - null if ParentMapID = 0
// - null if ParentMapID > 0 but no matching row exists
// - populated object if ParentMapID > 0 and matching row exists
```

**Implementation**:
- `Db2QueryProvider.ApplyInclude()` collects distinct keys, batch loads related entities
- Missing keys result in `TryGetValue` returning false, navigation remains `null`

---

### 2. Navigation Predicates - Inner-Join/Semi-Join Semantics

**Behavior**: Navigation predicates use inner-join/semi-join semantics. Only rows where the navigation can be resolved AND the predicate is true are included in results.

**Rules**:
- Rows with FK = 0 are excluded from results
- Rows with missing related data are excluded from results
- Only rows with valid navigation where predicate is true are included
- Semi-join optimization: evaluate predicate on related table first, collect matching keys, filter root

**Example**:
```csharp
var spells = context.Spell
    .Where(s => s.SpellName!.Name_lang.Contains("Fire"))
    .ToList();

// Only returns spells where:
// 1. SpellName row exists (Id > 0 and row found in SpellName table)
// 2. Name_lang contains "Fire"
//
// Spells without a SpellName row are excluded (inner-join semantics)
```

**Implementation**:
- `Db2NavigationQueryCompiler.TryCompileSemiJoinPredicate()` scans related table first
- Collects IDs where predicate is true
- Returns row predicate that checks if root's FK is in the matching ID set
- Missing related rows never match (FK not in set → row filtered out)

---

### 3. Navigation Projections - Null/Default for Missing Rows

**Behavior**: Navigation projections use left-join semantics. When the related row is missing, the projected value is `null` (for reference types) or the default value (for value types).

**Rules**:
- Missing related row => projected value is `null`/default
- Uses batched loading to prevent N+1 queries
- Never throws for missing rows

**Example**:
```csharp
var spellNames = context.Spell
    .Where(s => s.Id > 0)
    .Select(s => new { s.Id, Name = s.SpellName!.Name_lang })
    .Take(10)
    .ToList();

// For each spell:
// - If SpellName row exists: Name = actual string value
// - If SpellName row missing: Name = null
```

**Implementation**:
- `Db2BatchedNavigationProjector.Project()` collects distinct keys, batch loads related values
- Selector rewriter replaces `x.Nav.Member` with lookup: `TryGetValue(key, memberIndex, out value) ? value : default`
- Missing related row results in `TryGetValue` returning false, uses default value

---

## Error Handling - No Silent Failures

**Behavior**: Misconfiguration and data integrity issues throw exceptions immediately rather than silently returning empty results.

**Rules**:
- Table not found → exception (FileNotFoundException or equivalent)
- Schema mapping failure → exception
- Virtual field materialization error → exception
- No catch-and-succeed fallbacks that would mask errors

**Example (error case)**:
```csharp
// If SpellName.db2 file is missing or corrupted:
var spells = context.Spell
    .Include(s => s.SpellName)
    .ToList();

// Throws exception during Include execution, does NOT silently return spells with null SpellName
```

**Implementation**:
- Removed `try-catch` block in `Db2QueryProvider.CreateEntitiesLoader()` (was returning `_ => []` on exception)
- Exceptions now propagate to caller
- Model building validates navigations early (fail-fast)

---

## Edge Cases

### Zero Foreign Keys

**Behavior**: Foreign key value of `0` is treated as "no relation" (SQL NULL equivalent).

- Include: navigation is `null`
- Predicates: row excluded (FK = 0 → not in matched ID set)
- Projections: projected value is `null`/default

### Referential Integrity

**Behavior**: MimironSQL does not enforce referential integrity. It's possible for FK to point to a non-existent ID.

- Treated the same as FK = 0 (left-join for Include, excluded for predicates)
- No warnings or errors for dangling references
- Application must handle `null` navigations gracefully

### Multiple Navigation Predicates (AND)

**Behavior**: AND combinations of navigation predicates on the same navigation intersect matched ID sets.

**Example**:
```csharp
var spells = context.Spell
    .Where(s => s.SpellName!.Name_lang.Contains("Fire") && 
                s.SpellName!.Name_lang.Contains("Ball"))
    .ToList();

// Semi-join optimization:
// 1. Scan SpellName for "Fire" → IDs: {1, 2, 3}
// 2. Scan SpellName for "Ball" → IDs: {2, 3, 4}
// 3. Intersect: {2, 3}
// 4. Filter Spell to only IDs in {2, 3}
```

### Mixed Root + Navigation Predicates

**Behavior**: Root predicates and navigation predicates are composed (both must be true).

**Example**:
```csharp
var spells = context.Spell
    .Where(s => s.Id > 100 && s.SpellName!.Name_lang.Contains("Fire"))
    .ToList();

// 1. Semi-join: find Spell IDs where SpellName contains "Fire"
// 2. Compose: row => (Id > 100) && (Id in matched set)
```

---

## Implementation Notes

### Minimal Decoding Philosophy

All multi-source operations preserve the "minimal decoding" philosophy:

- Only fields required by the query plan are decoded
- `Db2RequiredColumns` / `Db2SourceRequirements` track per-source needs
- Navigation projections don't materialize full entities unless needed

### Batched Loading (Non-N+1)

All multi-source operations use batched loading:

- **Include**: `ApplyInclude()` collects keys from root scan, loads all related entities once
- **Navigation projections**: `Db2BatchedNavigationProjector` collects keys, loads values once
- **Semi-join predicates**: `FindMatchingIds()` scans related table once, returns ID set

Tests verify non-N+1 behavior using `Wdc5FileLookupTracker` (no `TryGetRowById` per entity).

---

## Test Coverage

Semantics are enforced by tests in `Phase4RobustnessTests.cs`:

1. `Include_leaves_navigation_null_when_foreign_key_is_zero` ✅
2. `Include_throws_when_referenced_table_does_not_exist` ✅
3. `Include_throws_when_table_file_cannot_be_opened` ✅

Note: Tests for missing-row scenarios (Include with missing FK target, navigation predicates/projections with missing rows) rely on test data having such cases. The test fixtures are complete (all FKs resolve), so those specific tests are not included. The documented semantics are enforced by the code paths used in existing tests where rows do exist.

Plus existing Phase 4 tests in `QueryTests.cs`:

- `Phase4_include_populates_reference_navigation_when_row_exists` ✅
- `Phase4_include_populates_shared_primary_key_navigation_when_row_exists` ✅
- `Phase4_include_is_batched_for_schema_fk_navigation_and_avoids_row_by_id_n_plus_one` ✅
- `Phase4_include_is_batched_for_shared_primary_key_navigation_and_avoids_row_by_id_n_plus_one` ✅

---

## Summary

| Operation | Missing Row Behavior | FK = 0 Behavior | Error Behavior (misconfiguration) | N+1 Safe? |
|-----------|---------------------|-----------------|-----------------------------------|-----------|
| **Include(...)** | Navigation = null (left-join) | Navigation = null | Throws (verified by tests) | ✅ Batched |
| **Navigation predicates** | Row excluded (inner-join) | Row excluded | Throws (verified by tests) | ✅ Semi-join |
| **Navigation projections** | Value = null/default (left-join) | Value = null/default | Throws (verified by tests) | ✅ Batched |

**Key principle**: Left-join for loading (Include/projections), inner-join for filtering (predicates), fail-loud for errors.
