## Plan: Db2ColumnIndex — B+-Tree Column Indexes for DB2 Files

### Overview

Instead of scanning raw record bytes at query time, pre-build persistent B+-tree index files for non-ID scalar columns at app startup. Indexes are stored in an auto-resolved app-data cache directory, keyed by table name + column + WoW version + layout hash. Once built, they're loaded on-demand per query, enabling O(log N) equality and range lookups that return `RowHandle`s directly — no row materialization or full-table scanning required.

Since DB2 data is read-only, the B+-tree never needs inserts or updates after initial construction. This makes the build phase a single bulk-load pass (sorted insert) and the read phase a simple page-walking binary search over memory-mapped files.

---

### Architecture

```
WithCustomIndexes()  ──►  App Startup  ──►  Db2IndexBuilder
   (builder ext)           (eager)          • inspects EF model
                                            • opens each DB2 file
                                            • for each non-ID scalar column:
                                            │   check cache (version + layout hash)
                                            │   if stale/missing → build B+-tree
                                            │   write .db2idx file to cache dir
                                            ▼
                                       Db2ColumnIndex (on-disk B+-tree)
                                            • 4KB pages, memory-mapped reads
                                            • leaf pages: sorted (value, sectionIndex, rowIndex) tuples
                                            • internal pages: separator keys + child page offsets
                                            ▼
                          Query Time  ──►  Db2IndexLookup
                                            • load index file (memory-map)
                                            • binary search B+-tree
                                            • return matching RowHandle(s)
                                            • dispose after query
```

---

### Steps

#### Phase 1: B+-Tree File Format

1. **Create `src/MimironSQL.Formats.Wdc5/Index/Db2IndexFileFormat.cs`.**
   Define the on-disk format as structs:

   - **File header** (fixed size, page 0):
     - Magic bytes (`DB2I`)
     - Format version (`1`)
     - WoW version string (fixed-length, null-padded, e.g. 32 bytes)
     - Layout hash (`uint`)
     - Table name (fixed-length, null-padded)
     - Column field index (`int`)
     - Value type (`Db2ValueType` as `byte`)
     - Value byte width (`byte`) — 1/2/4/8 depending on the column's storage size
     - Page size (`int`, always 4096)
     - Root page offset (`long`)
     - Total record count (`int`)
     - Tree height (`int`)

   - **Internal page** (4KB):
     - Page type marker (`0x01`)
     - Key count (`ushort`)
     - Keys: `keyCount` values of `valueBytesWidth` each
     - Child pointers: `keyCount + 1` page offsets (`long` each)
     - Remaining bytes: padding

   - **Leaf page** (4KB):
     - Page type marker (`0x02`)
     - Entry count (`ushort`)
     - Next-leaf page offset (`long`, for range scans; `0` = last leaf)
     - Entries: `entryCount` × `(value [valueBytesWidth], sectionIndex [ushort], rowIndexInSection [int])`
     - Remaining bytes: padding

   Entry size determines the B+-tree fanout:
   - For a 4-byte value: entry = 4 + 2 + 4 = 10 bytes → ~406 entries per leaf page
   - For an 8-byte value: entry = 8 + 2 + 4 = 14 bytes → ~290 entries per leaf page
   - Internal page fanout for 4-byte keys: ~(4096 - header) / (4 + 8) ≈ 336 children

2. **Create `src/MimironSQL.Formats.Wdc5/Index/Db2IndexWriter.cs`.**
   Internal class that builds a B+-tree index file from a sorted stream of `(value, sectionIndex, rowIndexInSection)` tuples:
   - Accepts an `IEnumerable<(ulong encodedValue, ushort sectionIndex, int rowIndex)>` (pre-sorted by value, then by section+row for stability).
   - Writes leaf pages sequentially, filling each to capacity.
   - After all leaves are written, builds internal pages bottom-up (bulk-load pattern — since data arrives sorted, the tree is built in a single pass with no splits).
   - Links leaf pages via next-leaf pointers for range scans.
   - Writes the file header last (or seeks back to update root offset).
   - Uses `FileStream` with `SequentialScan` hint for build, then closes.

3. **Create `src/MimironSQL.Formats.Wdc5/Index/Db2IndexReader.cs`.**
   Internal `IDisposable` class that reads a B+-tree index via memory-mapped file:
   - Opens the file with `MemoryMappedFile.CreateFromFile()` + `CreateViewAccessor()`.
   - Validates the header (magic, format version).
   - **`FindEquals(T value) → List<RowHandle>`**: binary search from root → leaf, then scan matching entries in the leaf (and linked leaves if the value spans pages). Construct `RowHandle(sectionIndex, rowIndex, rowId: 0)` — row IDs are resolved lazily by the caller if needed.
   - **`FindRange(T low, T high) → List<RowHandle>`**: find the leaf containing `low`, walk linked leaves until `high` is exceeded.
   - Disposes the memory-mapped file when the reader is disposed.
   - All reads are via `ReadOnlySpan<byte>` from the memory-mapped view — zero-copy.

#### Phase 2: Index Builder (Startup)

4. **Create `src/MimironSQL.EntityFrameworkCore/Index/Db2IndexBuilder.cs`.**
   Internal class, registered as singleton. Builds indexes eagerly at startup.
   - Injected dependencies: `IMimironDb2Store`, `IDb2ModelBinding`, `IDb2Format`.
   - **`BuildIndexes(IModel model, string wowVersion)`**:
     - Resolve the app-data cache directory: `Path.Combine(Environment.GetFolderPath(SpecialFolder.LocalApplicationData), "MimironSQL", "indexes", wowVersion)`.
     - For each entity type in the model:
       - Get `Db2EntityType` → `Db2TableSchema`
       - For each `Db2FieldSchema` in the schema where `!IsId && !IsVirtual && !IsRelation && ValueType is not (String or LocString)`:
         - Compute index file name: `{tableName}_{fieldName}_{layoutHash:X8}.db2idx`
         - If the file already exists → read its header → check WoW version and layout hash match → if valid, skip.
         - Otherwise, open the DB2 file, extract all `(value, sectionIndex, rowIndex)` tuples for that column (using `ReadField<T>` or direct `Wdc5FieldDecoder` access), sort them, and write the B+-tree via `Db2IndexWriter`.
   - Non-ID scalar columns are: `ValueType` of `Int64`, `UInt64`, `Single` (excludes `String`, `LocString`, and the PK ID field).
   - Building in parallel per table (or per column) is safe since each index file is independent.

5. **Create `src/MimironSQL.EntityFrameworkCore/Index/Db2IndexCacheLocator.cs`.**
   Small utility class that resolves and ensures the cache directory exists:
   - `GetIndexDirectory(string wowVersion) → string`
   - Creates the directory if it doesn't exist.
   - Returns absolute path.

6. **Value encoding.**
   Values must be encoded as unsigned integers for correct B+-tree ordering:
   - `int` → flip sign bit (`value ^ 0x80000000`) so that negative values sort before positive
   - `uint` → identity
   - `short` / `ushort` / `byte` / `sbyte` → widen to `uint` with sign-flip if signed
   - `float` → IEEE 754 sortable encoding: if sign bit is set, flip all bits; else flip sign bit only
   - `double` → same IEEE 754 treatment, 8 bytes
   - `long` / `ulong` → sign-flip for signed, identity for unsigned

   Create `Db2IndexValueEncoder` (internal static class) with `Encode<T>(T value) → ulong` and `Decode<T>(ulong encoded) → T`.

#### Phase 3: Query Integration

7. **Create `src/MimironSQL.EntityFrameworkCore/Index/Db2IndexLookup.cs`.**
   Internal class used by the query pipeline to check for and use available indexes:
   - **`TryGetIndex(string tableName, int fieldIndex, string wowVersion, string layoutHash) → Db2IndexReader?`**: checks the cache directory for the index file, opens it if present, returns `null` if not found.
   - Returns a `Db2IndexReader` — caller is responsible for disposal.

8. **Integrate with `Db2TableEnumerator.Table()`.**
   Before falling back to full-table scan, check if the predicate is a simple equality on a single indexed column:
   - If `Db2RowPredicateCompiler` identifies a single-field equality conjunct (already done for PK via `TryGetVirtualIdPrimaryKeyEqualityLookup`), extract the field index and constant value.
   - Call `Db2IndexLookup.TryGetIndex(...)` — if an index exists, use `Db2IndexReader.FindEquals()` to get `RowHandle`s directly.
   - Materialize only the matched rows. Remaining predicates (if any) are still applied as a row predicate on the subset.
   - If no index exists, fall through to the existing full-scan path.

9. **Row ID resolution.**
   `Db2IndexReader.FindEquals()` returns `RowHandle(sectionIndex, rowIndex, rowId: 0)` since the index doesn't store row IDs. Two options for resolution:
   - The caller resolves the row ID after lookup via `IDb2File.ReadField<int>(handle, idFieldIndex)` — one field read per matched row, still much cheaper than full materialization.
   - Alternative: store row IDs in the index entries (increases entry size by 4 bytes but avoids the post-lookup read). **Decision**: start without row IDs in the index (smaller files, simpler format). Add them if profiling shows the post-lookup ID read is a bottleneck.

#### Phase 4: Service Registration

10. **Add `WithCustomIndexes()` extension method.**
    On `IMimironDb2DbContextOptionsBuilder` (in Contracts) or as an extension method on `MimironDb2DbContextOptionsBuilder` (in EntityFrameworkCore):
    ```csharp
    public static IMimironDb2DbContextOptionsBuilder WithCustomIndexes(
        this IMimironDb2DbContextOptionsBuilder builder)
    ```
    - Sets a flag on `MimironDb2OptionsExtension` (`EnableCustomIndexes = true`).
    - When `ApplyServices()` runs: registers `Db2IndexBuilder` (singleton), `Db2IndexCacheLocator` (singleton), `Db2IndexLookup` (singleton).

11. **Startup hook.**
    Use `IHostedService` or `IStartupFilter` (or a simpler approach: an `EnsureCreated`-style method) to trigger `Db2IndexBuilder.BuildIndexes()` at startup:
    - Option A: Register an `IHostedService` that resolves `Db2IndexBuilder` and calls `BuildIndexes()` during `StartAsync()`. Indexed tables and columns are determined from the EF model.
    - Option B: Call `Db2IndexBuilder.BuildIndexes()` lazily on first `DbContext` creation (via the model customizer or store constructor). This delays the build until the model is finalized.
    - **Decision**: Option B (lazy on first DbContext) is simpler and doesn't require `IHostedService` dependency. The build runs once, is thread-safe (use `Lazy<Task>` or a lock), and subsequent contexts skip it.

#### Phase 5: Tests

12. **Unit tests: B+-tree format (`tests/MimironSQL.Formats.Wdc5.Tests/Index/`).**
    - `Db2IndexWriterTests`: build an index from known sorted data, verify file header, page structure, entry count.
    - `Db2IndexReaderTests`: write an index, open with reader, verify `FindEquals` returns correct entries.
    - `Db2IndexValueEncoderTests`: verify encoding/decoding round-trips for all numeric types; verify sort order is preserved (encoded values maintain the same ordering as the original typed values).
    - Range queries: `FindRange(low, high)` returns correct subset.
    - Edge cases: empty index, single entry, value spanning multiple leaf pages, maximum fanout.

13. **Integration tests: end-to-end index build + query (`tests/MimironSQL.EntityFrameworkCore.Tests/`).**
    - Configure `WithCustomIndexes()`, run a query with an equality predicate on a non-ID scalar column, verify correct results.
    - Verify index file is created in the expected cache directory.
    - Verify staleness: change layout hash, verify index is rebuilt.
    - Verify that queries without matching indexes fall through to full scan.

14. **`InternalsVisibleTo` access.**
    Ensure test projects can access internal types. Verify existing `InternalsVisibleTo` declarations cover the test projects; add if missing.

---

### Staleness Detection

Each index file's header contains:
- **WoW version string** (from `MimironDb2OptionsExtension.WowVersion`)
- **Layout hash** (from `Db2FileLayout.LayoutHash`)

At startup, `Db2IndexBuilder` reads just the header of each existing index file and compares against the current WoW version and the DB2 file's actual layout hash. If either differs, the index is rebuilt. If both match, the index is reused.

This means:
- WoW patch with new DB2 data (new layout hash) → indexes rebuild automatically.
- Same WoW version, same DB2 files → indexes are reused across app restarts.
- Adding new entities or properties to the EF model → only new indexes are built; existing ones are untouched.
- Removing entities/properties → orphaned index files remain on disk (harmless; could add optional cleanup).

---

### File Layout Example

For a table `Map` with WoW version `12.0.0.65655`, layout hash `A1B2C3D4`, field `AreaTableID` (field index 3, `int`):
```
Cache dir: %LOCALAPPDATA%/MimironSQL/indexes/12.0.0.65655/
File:      Map_AreaTableID_A1B2C3D4.db2idx
```

File structure (conceptual):
```
Page 0: Header
  Magic: DB2I
  Version: 1
  WowVersion: "12.0.0.65655"
  LayoutHash: 0xA1B2C3D4
  TableName: "Map"
  FieldIndex: 3
  ValueType: Int64 (mapped from the DBD)
  ValueByteWidth: 4
  PageSize: 4096
  RootPageOffset: 4096  (page 1)
  RecordCount: 4500
  TreeHeight: 2

Page 1: Internal (root)
  Keys: [500, 1200, 2300, ...]
  Children: [page 2, page 3, page 4, ...]

Pages 2..N: Leaf pages
  Entries: [(encodedValue, sectionIndex, rowIndex), ...]
  NextLeaf: offset to next leaf page (or 0)
```

---

### Decisions

- **B+-tree with 4KB pages**: classic design for read-heavy workloads. Memory-mapped reads avoid syscalls after initial mapping. 4KB aligns with OS page size for optimal I/O.
- **Non-ID scalar columns only**: ID already has runtime hash-index (`_idIndex`). String/LocString columns need different treatment (string block scanning). FK columns that reference IDs are scalar and will be indexed.
- **App-data cache directory**: auto-resolved, no user configuration needed, survives app restarts, doesn't require write access to the DB2 source directory.
- **Hybrid lifecycle**: eager build on first `DbContext` creation (one-time cost). CLI tool deferred to a future enhancement — would require a pre-configured model/entity set.
- **No row IDs in index entries**: keeps entries compact (10 bytes for 4-byte values). Row IDs are resolved post-lookup via a single `ReadField` call per matched row.
- **Unsigned encoding for sort order**: sign-bit flipping ensures signed integers and floats sort correctly in the B+-tree's unsigned comparison order.
- **Per-column index files**: each column gets its own file. Avoids rebuilding unrelated indexes when adding new entities/properties. Enables independent loading per query.
- **Future enhancement — user-configurable per entity**: `WithCustomIndexes(o => o.ForEntity<Map>(e => e.IndexColumn(m => m.AreaTableID)))`. Not in scope for the initial implementation; the default indexes all non-ID scalar columns.

---

### Verification

- `dotnet build MimironSQL.slnx` — compilation succeeds.
- `dotnet test MimironSQL.slnx` — all existing tests pass; new index tests pass.
- Correctness baseline: for each indexed column, verify that `Db2IndexReader.FindEquals(value)` returns the same set of `RowHandle`s as `EnumerateRowHandles().Where(h => file.ReadField<T>(h, fieldIndex).Equals(value))`.
- Staleness: verify that changing the WoW version or layout hash triggers a rebuild.
- Performance (optional benchmark): compare index-assisted equality lookup vs full-table scan + row predicate for a table with 10K+ rows.
