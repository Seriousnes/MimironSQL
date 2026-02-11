# Plan: Eliminate DRY Violations in WDC5 Format Project

## Problem Statement

The `MimironSQL.Formats.Wdc5` project contains three classes that significantly overlap in purpose and implementation:

| Class | Lines | Purpose |
|-------|-------|---------|
| `Wdc5File` | ~1327 | Full in-memory WDC5 file. Implements `IDb2File<RowHandle>`. Parses everything (header, sections, record data, string tables, id index, copy table). Supports full enumeration and random-access by ID. |
| `Wdc5KeyLookupMetadata` | ~455 | Header + section metadata parser. Parses header, section headers, field/column meta, pallet/common data, index data, and copy tables — but **skips record data bytes** (seeks past them). Builds the `_idIndex` and `_copyMap` eagerly. Has `TryResolveRowHandle()` for ID resolution. Contains its own `Section` class and `RowResolution` record. |
| `Wdc5KeyLookupRowFile` | ~701 | Single-row `IDb2File<RowHandle>` implementation. Wraps a `Wdc5KeyLookupMetadata` and reads just one row's bytes from a stream on-demand. Implements full `ReadField<T>`, string reading, decryption — all duplicated from `Wdc5File`. |

### Quantified Duplication

~400 lines of code are **effectively copy-pasted** between `Wdc5File` and `Wdc5KeyLookupRowFile`:

| Method | Wdc5File | Wdc5KeyLookupRowFile | Status |
|--------|----------|---------------------|--------|
| `CastVirtualField<T>` | L751–793 | L330–377 | **Identical** |
| `ReadScalarTyped<T>` | L795–860 | L379–440 | **Identical** |
| `ReadFieldTyped<T>` | L697–749 | L236–283 | Near-identical |
| `MoveToFieldStart` | L1243–1251 | L443–454 | Minor difference |
| `TryGetString` | L1084–1089 | L456–463 | Identical structure |
| `TryGetDenseString` | L1090–1140 | L465–519 | Same algorithm, different data source |
| `TryGetInlineString` | L1142–1162 | L521–537 | **Identical** |
| `SkipSparseField` | L1164–1196 | L539–577 | Near-identical |
| `TryReadNullTerminatedUtf8` | L1200–1230 | L579–603 | Slight bounds diff |
| `ReadArray<T>` | L1066–1082 | L618–631 | **Identical** |
| `ReadNoneArray<T>` | L1253–1267 | L633–646 | **Identical** (static) |
| `ReadPalletArray<T>` | L1269–1281 | L648–660 | **Identical** (static) |
| `GetArrayBoxed` | L1042–1064 | L662–682 | Near-identical |
| `DecryptedRowLease` struct | L1283–1296 | L191–210 | **Identical** |
| `DecryptRowBytes` | L1298–1327 | L213–234 | **Identical** |
| ID conversion in `TryGetRowHandle` | L453–466 | L68–81 | **Identical** |

Additionally, `Wdc5KeyLookupMetadata.Parse()` duplicates ~200 lines of header parsing, section header parsing, field/column meta parsing, pallet/common data parsing, and section iteration logic from `Wdc5File`'s constructor.

### Additional Concerns
- `Wdc5KeyLookupMetadata` has a `public static Parse()` method that acts as a factory, violating the stated constraint that all file operations must go through an instance of `Wdc5File`
- `Wdc5KeyLookupMetadata` defines its own `Section` class, separate from `Wdc5Section`, with overlapping-but-not-identical fields
- `Wdc5KeyLookupRowFile` is only constructed by `MimironDb2Store` (the EFCore layer tightly coupled to WDC5)

## Design Goal

**All WDC5 file operations must go through a single `Wdc5File` instance.** `Wdc5KeyLookupMetadata` and `Wdc5KeyLookupRowFile` must be eliminated. `Wdc5File` must support both:
1. **Full enumeration** (current behavior — load all record data eagerly)
2. **Key-lookup** (lazy — parse header + metadata + id index eagerly, load record data on-demand for individual rows)

No public static methods are allowed (extension methods excepted).

## Proposed Changes

### Step 1: Make `Wdc5File` support lazy record data loading

Currently, the `Wdc5File` constructor always reads all record bytes into `Wdc5Section.RecordsData` (`byte[]`). For key-lookup, we only need:
- Header + section headers + field/column meta + pallet/common data (already parsed)
- Id index + copy table (already lazily built via `EnsureIndexesBuilt()`)
- Individual row bytes on-demand

Add a "lazy mode" to `Wdc5File`:
- The constructor always parses header, section headers, field/column meta, pallet/common data
- **Lazy mode**: Skip reading record data bytes (seek past them). Record bytes for individual rows are read from the stream on-demand when `ReadField<T>` is called.
- **Eager mode** (default, current behavior): Read all record data + string tables into memory as today.

Options for implementing lazy mode:
- A `Wdc5FileOptions` flag (e.g., `LazyRecordLoading = true`)
- A separate factory method that returns the same `Wdc5File` type but with lazy internal state
- The file always starts lazy and materializes record blobs on first enumeration

**Recommendation**: Add a `bool lazyRecordLoading` option (or a `Wdc5FileOptions.RecordLoadingMode` enum). When lazy:
- `Wdc5Section.RecordsData` is `null` (or `ReadOnlyMemory<byte>.Empty`)
- The `_stream` reference is stored (the file takes ownership, caller should not close it)
- `ReadField<T>` reads from the stream on-demand (similar to how `Wdc5KeyLookupRowFile` does it today)
- `EnumerateRows()` could either throw (if truly lazy single-row mode) or trigger a full load

### Step 2: Eagerly build the id index

Currently `Wdc5File.EnsureIndexesBuilt()` is lazy — called on first `TryGetRowHandle`. For the key-lookup use case, the id index must be available immediately after construction. Options:
- Build the id index eagerly in the constructor (from index data arrays, which are already parsed regardless of record blob loading)
- Keep it lazy but ensure it doesn't require record data

The id index is built from `Wdc5Section.IndexData` (parsed during section iteration in the constructor) and `Wdc5Section.CopyData`. Both are available without reading record bytes. **The current lazy approach can remain**, but ensure `EnsureIndexesBuilt()` does not depend on `RecordsData`.

Actually, looking at the current `EnsureIndexesBuilt()` implementation: it iterates sections, calls `CreateReaderAtRowStart` and `GetVirtualId`. `CreateReaderAtRowStart` accesses `RecordsData`. However, `Wdc5KeyLookupMetadata` builds its index from `IndexData` arrays directly — it never reads record bytes. **The id index building in `Wdc5File` should be changed to use `IndexData` arrays** (like `Wdc5KeyLookupMetadata` does), not record data.

### Step 3: Consolidate `Section` types

Currently:
- `Wdc5Section` (used by `Wdc5File`) — holds `RecordsData`, `StringTableData`, `IndexData`, `CopyData`, `ParentLookupEntries`, etc.
- `Wdc5KeyLookupMetadata.Section` — holds `Header`, `FirstGlobalRecordIndex`, `RecordDataSizeBytes`, `RecordsBaseOffsetInBlob`, `StringTableBaseOffset`, `IndexData`, `CopyData`, `ParentLookupEntries`, `SparseEntries`, `SparseRecordStartBits`, `TactKey`

These should be unified into `Wdc5Section`. Add any missing fields from `Wdc5KeyLookupMetadata.Section` to `Wdc5Section`:
- `FirstGlobalRecordIndex`
- `RecordDataSizeBytes`
- `RecordsBaseOffsetInBlob`
- `StringTableBaseOffset`
- `TactKey`

Make `RecordsData` and `StringTableData` nullable or `ReadOnlyMemory<byte>` (empty when in lazy mode).

### Step 4: Move `TryResolveRowHandle` logic into `Wdc5File`

`Wdc5KeyLookupMetadata.TryResolveRowHandle(requestedId, out handle, out resolution)` provides:
- Copy-table redirect
- Id-index lookup
- `RowResolution` with source/destination IDs, parent relation, global row index

This logic should live in `Wdc5File`. The existing `TryGetRowHandle<TId>` already does the copy-redirect + id-index lookup but returns only a `RowHandle`. Extend it to also produce enough context for `ReadField<T>` to work in lazy mode:
- `RowHandle` already contains `(SectionIndex, RowIndexInSection, RowId)`
- The `ReadField<T>` method already resolves source/destination IDs and parent relation from the handle

So `TryGetRowHandle` + `ReadField` already provide the same capability as `TryResolveRowHandle` — no additional method may be needed. Just ensure `ReadField<T>` works correctly in lazy mode (reads from stream instead of in-memory blob).

### Step 5: Implement on-demand row reading in `ReadField<T>`

When in lazy mode, `ReadField<T>` should:
1. Compute the file offset for the requested row (using section file offsets + row index × record size, or sparse entry offsets)
2. Seek the stream and read just that row's bytes into a temporary buffer
3. Create a `Wdc5RowReader` over the buffer
4. Proceed with the existing field decoding logic

This is exactly what `Wdc5KeyLookupRowFile.ReadRowBytes()` + `ReadField<T>` does today. The implementation moves into `Wdc5File`.

For dense string reading in lazy mode: the dense string table may not be in memory. The file should either:
- Read strings from the stream on-demand (like `Wdc5KeyLookupRowFile.TryReadNullTerminatedUtf8FromStream`)
- Or load the string table lazily on first string read

### Step 6: Eliminate `Wdc5KeyLookupMetadata` and `Wdc5KeyLookupRowFile`

Once `Wdc5File` supports lazy mode with on-demand row reading:
- Delete `Wdc5KeyLookupMetadata.cs`
- Delete `Wdc5KeyLookupRowFile.cs`
- Update all callers (primarily `MimironDb2Store.TryMaterializeById`) to use `Wdc5File` with lazy mode
- Actually, after the tight-coupling plan is implemented, the EFCore layer won't reference `Wdc5File` at all — it will use `IDb2Format.OpenFile()` which internally creates a `Wdc5File`

### Step 7: Extract shared field-reading logic

The duplicated methods (`CastVirtualField<T>`, `ReadScalarTyped<T>`, `ReadArray<T>`, etc.) currently exist in both `Wdc5File` and `Wdc5KeyLookupRowFile`. After consolidation into `Wdc5File`, there will be only one copy. However, consider extracting pure helper methods into a static helper class (e.g., `Wdc5FieldHelpers`) if `Wdc5File` becomes too large:
- `CastVirtualField<T>` — pure, no state
- `ReadScalarTyped<T>` — delegates to `Wdc5FieldDecoder`
- `ReadNoneArray<T>`, `ReadPalletArray<T>` — already static

This is optional — if `Wdc5File` remains cohesive, keeping them as private methods is fine.

## Implementation Order

1. **Add missing fields to `Wdc5Section`** (Step 3) — non-breaking, additive
2. **Fix id index building to use `IndexData` arrays** (Step 2) — behavioral change, must maintain existing test coverage
3. **Add lazy record loading mode** (Step 1 + Step 5) — new functionality, add tests
4. **Move `TryResolveRowHandle` equivalence into `Wdc5File`** (Step 4) — verify via existing tests
5. **Delete `Wdc5KeyLookupMetadata` and `Wdc5KeyLookupRowFile`** (Step 6) — requires tight-coupling plan to be complete first
6. **Optional: extract helpers** (Step 7)

## Testing Strategy

- All existing `Wdc5File` unit tests must pass — they exercise the eager (full-load) path  
- All existing integration tests (including `Can_query_db2context_for_spell` and `Key_lookup_does_not_populate_full_table_cache`) must pass — they exercise the key-lookup path
- New unit tests for lazy mode:
  - Open a file in lazy mode, verify `TryGetRowHandle` works
  - Read a single field via `ReadField<T>` in lazy mode, verify correct value
  - Verify lazy mode doesn't allocate record blob memory
  - Verify dense string reading works in lazy mode (from stream)
  - Verify sparse file support in lazy mode
  - Verify encrypted row decryption in lazy mode

## Acceptance Criteria
- [ ] `Wdc5KeyLookupMetadata.cs` is deleted
- [ ] `Wdc5KeyLookupRowFile.cs` is deleted
- [ ] `Wdc5File` supports lazy record loading via an option
- [ ] Id index building does not require record data bytes
- [ ] `ReadField<T>` works in both eager and lazy modes
- [ ] No public static factory methods on `Wdc5File` (the constructor is the only entry point)
- [ ] `Wdc5Section` is the single section type (no duplicate `Section` class)
- [ ] Zero duplicated field-reading logic across the WDC5 project
- [ ] All existing tests pass unchanged
- [ ] New tests cover lazy mode scenarios
