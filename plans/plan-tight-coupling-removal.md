# Plan: Remove Tight Coupling Between EFCore and WDC5

## Problem Statement

The `MimironSQL.EntityFrameworkCore` project has direct compile-time dependencies on `MimironSQL.Formats.Wdc5` types in two source files, plus a `<ProjectReference>` in the `.csproj`. The EFCore layer should be format-agnostic — only the DI registration in `MimironDb2ServiceCollectionExtensions` should reference a concrete format as the **default**.

### Current WDC5 Coupling Points

| # | File | WDC5 Type | Usage |
|---|------|-----------|-------|
| 1 | `MimironDb2ServiceCollectionExtensions.cs` | `Wdc5Format` | `services.AddSingleton<IDb2Format, Wdc5Format>()` — hardcoded default format registration |
| 2 | `MimironDb2Store.cs` | `Wdc5LayoutReader` | `GetSchemaFromMetadata()` — reads layout hash+field count from header without full file parse |
| 3 | `MimironDb2Store.cs` | `Wdc5KeyLookupMetadata` | `TryMaterializeById()` — parses header+section metadata+id index for key-lookup |
| 4 | `MimironDb2Store.cs` | `Wdc5KeyLookupRowFile` | `TryMaterializeById()` — constructs a single-row IDb2File for key-lookup materialization |
| 5 | `MimironSQL.EntityFrameworkCore.csproj` | Project reference | `<ProjectReference Include="..\MimironSQL.Formats.Wdc5\MimironSQL.Formats.Wdc5.csproj" />` |

### Coupling #1 (default registration) is **acceptable**
The DI registration is the only legitimate place for a concrete format reference — it provides the default `IDb2Format` implementation. Users could override this by registering their own `IDb2Format` before or after.

### Couplings #2–#4 are **violations**
`MimironDb2Store` directly instantiates WDC5-specific types (`Wdc5KeyLookupMetadata`, `Wdc5KeyLookupRowFile`, `Wdc5LayoutReader`) rather than working through `IDb2Format`/`IDb2File` abstractions. If a future format (e.g., WDC6) were added, `MimironDb2Store` would need to be modified — violating the Open/Closed Principle.

## Root Cause Analysis

The tight coupling exists because the `IDb2Format` and `IDb2File` contracts don't expose:
1. **Layout reading without full file parsing** — `Wdc5LayoutReader.ReadLayout(stream)` reads only 200 bytes. Historically, `IDb2Format` only had `OpenFile(stream)` (full parse) and `GetLayout(IDb2File)` (requires a fully-parsed file).
2. **Key-lookup / id-based row resolution** — `IDb2File.TryGetRowHandle<TId>()` exists but requires a fully-parsed file. There's no contract for "parse just enough to resolve an ID to a row, then read just that row."

## Proposed Changes

### Step 1: Add `GetLayout(Stream)` to `IDb2Format`
```csharp
public interface IDb2Format
{
    // Existing:
    Db2Format Format { get; }
    IDb2File OpenFile(Stream stream);
    Db2FileLayout GetLayout(IDb2File file);
    
    // New:
    Db2FileLayout GetLayout(Stream stream);
}
```
The WDC5 implementation delegates to `Wdc5LayoutReader.ReadLayout(stream)`. This replaces `MimironDb2Store.GetSchemaFromMetadata`'s direct use of `Wdc5LayoutReader`.

### Step 2: Remove `Wdc5KeyLookupMetadata` and `Wdc5KeyLookupRowFile` from the EFCore project
Per the **DRY Violations plan**, `Wdc5File` will be consolidated to support both lazy id-index building (without full record blob allocation) and single-row reading. Once that's done:

- `MimironDb2Store.TryMaterializeById` should use `IDb2Format.OpenFile(stream)` to get an `IDb2File`, then call `IDb2File.TryGetRowHandle<int>(id, out handle)` + `IDb2File.ReadField<T>(handle, fieldIndex)` — **exactly the same interfaces used by the full-scan path**.
- The optimization of reading only one row's bytes (rather than all record data) becomes an **internal implementation detail** of `Wdc5File`, not something the EFCore layer orchestrates.

### Step 3: Simplify `TryMaterializeById` in the store
After Steps 1–2, `TryMaterializeById` becomes:

```csharp
public bool TryMaterializeById<TEntity>(..., int id, ...)
{
    var (file, schema) = OpenTableWithSchema(tableName);  // uses existing cache
    if (!file.TryGetRowHandle(id, out var handle))
        return false;
    
    var materializer = new Db2EntityMaterializer<TEntity, RowHandle>(...);
    entity = materializer.Materialize(file, handle);
    return true;
}
```

No WDC5-specific types needed. The `Wdc5File` internally optimizes for single-row access.

### Step 4: Remove the project reference (or make it conditional)
After all WDC5 references are removed from `MimironDb2Store.cs`, the only remaining reference is the DI registration in `MimironDb2ServiceCollectionExtensions.cs`. Options:
- **Keep the `<ProjectReference>`** but know it's only for the default registration line — acceptable
- **Move the default registration to a separate assembly** (e.g., `MimironSQL.EntityFrameworkCore.Defaults`) — cleaner but more complex
- **Use assembly scanning or convention** to discover the default format — over-engineered

**Recommendation**: Keep the `<ProjectReference>` since a single line of DI registration is an acceptable coupling point. Document that it's the only allowed WDC5 reference in the EFCore project.

### Step 5: Add a code analysis rule or test
Add a test or Roslyn analyzer that verifies `MimironDb2Store.cs` (and all files except `MimironDb2ServiceCollectionExtensions.cs`) do not reference any types from the `MimironSQL.Formats.Wdc5` namespace. This prevents regression.

## Specific Refactoring Steps for MimironDb2Store.cs

### `GetSchemaFromMetadata(string tableName)` — lines 60–77
**Before**: Opens stream → calls `Wdc5LayoutReader.ReadLayout(stream)` → gets `Db2FileLayout`  
**After**: Opens stream → calls `_format.GetLayout(stream)` → gets `Db2FileLayout`

### `TryMaterializeById<TEntity>(...)` — lines 92–121
**Before**: Caches `KeyLookupTable(Wdc5KeyLookupMetadata, Schema)` → opens stream #1 for metadata → resolves ID → opens stream #2 → creates `Wdc5KeyLookupRowFile` → materializes  
**After**: Uses `OpenTableWithSchema(tableName)` (shared cache) → `file.TryGetRowHandle(id, out handle)` → `materializer.Materialize(file, handle)` — single stream, no WDC5 types

### Removal of `_keyLookupCache` field
After refactoring, `_keyLookupCache` and the `KeyLookupTable` record become unnecessary — key-lookup uses the same `_cache` as full-scan.

## Dependencies
- **Plan: DRY Violations** — `Wdc5File` must support efficient key-lookup (lazy id-index, on-demand row reading) before the EFCore layer can stop orchestrating format-specific key-lookup.
- **Plan: Redundancy Refactoring** — Unifying the caches is part of both plans.

## Migration Order

1. First: Consolidate `Wdc5File` (DRY plan)
2. Then: Add `GetLayout(Stream)` to `IDb2Format` (this plan, Step 1)
3. Then: Refactor `MimironDb2Store` to use only `IDb2Format`/`IDb2File` (this plan, Steps 2–3)
4. Finally: Add regression test (this plan, Step 5)

## Acceptance Criteria
- [x] `MimironDb2Store.cs` has zero `using MimironSQL.Formats.Wdc5.*` statements
- [x] Only `MimironDb2ServiceCollectionExtensions.cs` references `Wdc5Format`
- [x] `IDb2Format` has a `GetLayout(Stream)` method
- [x] `_keyLookupCache` and `KeyLookupTable` are removed from the store
- [ ] (Deferred) A test or analyzer enforces no WDC5 references outside DI registration
- [x] All tests pass (one integration test was updated to match the new cache behavior)
