## Plan: Wdc5RecordScanner — Binary Value Scanning Without Row Materialization

Instead of iterating `RowHandle`s and calling `ReadField<T>()` per row (which positions a bit-reader, dispatches by compression type, decodes, and boxes), this scanner finds matches directly in the raw `RecordsData` byte arrays. For dense files with byte-aligned fields, a vectorized `IndexOf` locates the byte pattern and integer division backtracks to the owning row. For bit-packed or compressed fields, equivalent shortcuts avoid full materialization.

The scanner lives in `src/MimironSQL.Formats.Wdc5/Db2/` as a WDC5-specific internal class. It returns `RowHandle`s that the existing pipeline (or a future EF Core integration) can use.

**Steps**

1. **Create `Wdc5RecordScanner` in [src/MimironSQL.Formats.Wdc5/Db2/](src/MimironSQL.Formats.Wdc5/Db2/).**  
   Internal class that takes a `Wdc5File` reference. Primary method: `ScanFieldEquals<T>(int fieldIndex, T value, List<RowHandle> results)` where `T : unmanaged`. The method dispatches to a strategy based on `ColumnMetaData.CompressionType` and field alignment.

2. **Strategy dispatch — choose scan path per column.**  
   Inspect `ColumnMeta[fieldIndex]` to determine the approach:
   - `CompressionType.None` / `Immediate` / `SignedImmediate` **and** byte-aligned (`RecordOffset % 8 == 0` **and** `bitWidth % 8 == 0`) → **vectorized byte search** (Step 3)
   - `CompressionType.None` / `Immediate` / `SignedImmediate` **and** bit-packed → **strided bit scan** (Step 4)
   - `CompressionType.Common` → **dictionary scan** (Step 5)
   - `CompressionType.Pallet` / `PalletArray` → **reverse pallet lookup + record scan** (Step 6)
   - Virtual ID field (`fieldIndex == Header.IdFieldIndex` when stored in `IndexData`) → **index array scan** (Step 7)

3. **Vectorized byte search (byte-aligned inline fields).**  
   For each section with non-null `RecordsData`:
   - Encode the search value `T` into a `Span<byte>` of exactly the field's storage width (`32 - FieldMeta[i].Bits` / 8 for `None`, or `Immediate.BitWidth / 8`), in little-endian byte order. For `SignedImmediate`, encode with the same two's-complement representation the format uses.
   - Compute `columnByteOffset = ColumnMeta[fieldIndex].RecordOffset / 8` and `fieldByteWidth = storageWidth / 8`.
   - Scan `section.RecordsData.AsSpan()` using `MemoryExtensions.IndexOf(needle)` starting from offset 0.
   - For each hit at byte position `pos`:
     - `posInRecord = pos % Header.RecordSize`
     - If `posInRecord != columnByteOffset` → false positive, continue from `pos + 1`.
     - Else → `rowIndex = pos / Header.RecordSize`. Construct `RowHandle(sectionIndex, rowIndex, resolvedId)` and add to results. Advance search to `pos + Header.RecordSize` (next valid column-aligned position).
   - Resolving `rowId`: use `section.IndexData[rowIndex]` if the file has `Db2Flags.Index`, otherwise decode from the record's ID field bits (existing `TryGetVirtualId` path).
   - **Optimisation note**: for fields ≤ 2 bytes, `IndexOf` generates excessive false positives. Add a heuristic: if `fieldByteWidth <= 2`, fall through to strided scan (Step 4) instead, since the alignment check rejects most matches and the overhead of repeated `IndexOf` restarts exceeds a linear stride.

4. **Strided bit scan (bit-packed inline fields, or short-value fallback).**  
   For each section with non-null `RecordsData`:
   - Compute the bit offset (`ColumnMeta[fieldIndex].RecordOffset`) and bit width for the field.
   - Encode the search value as a `ulong` (matching how `Wdc5FieldDecoder.ReadScalar<T>` produces the value — including sign extension for `SignedImmediate`).
   - Walk records at stride `Header.RecordSize` bytes (dense) or via `SparseRecordStartBits` (sparse).
   - At each record: construct a `Wdc5RowReader` at the record start, seek to `RecordOffset` bits, read the field's bit width, compare against the encoded search value.
   - On match → construct `RowHandle` and add to results.
   - This avoids the full `ReadField<T>()` dispatch (no generic virtual calls, no string handling, no per-row validation) — it's a tight loop over a single column.

5. **Common compression — dictionary scan (no record data involved).**  
   The column's `CommonData` is `Dictionary<int, uint>` mapping `rowId → value`, with `defaultValue = ColumnMeta[fieldIndex].Common.DefaultValue`.
   - Encode search value as `uint`.
   - If `searchUint == defaultValue`: every row whose ID is **not** a key in `CommonData` matches. Walk all `RowHandle`s (from `EnumerateRowHandles()` or by walking sections + index data), filtering out IDs present in `CommonData`.
   - If `searchUint != defaultValue`: iterate `CommonData` entries, collect row IDs where `entry.Value == searchUint`, then resolve each ID to a `RowHandle` via `TryGetRowHandle`.

6. **Pallet / PalletArray — reverse lookup + record scan.**  
   - `PalletData` for the field is `uint[]`. Scan it to find all pallet indices `i` where `PalletData[i] == searchValueAsUint`. For `PalletArray` with `Cardinality: 1`, same logic.
   - If no pallet indices match → zero results, return immediately.
   - If one index matches → encode that index as the new search value, then dispatch to Step 3 or 4 (searching the record data for the pallet index rather than the original value). The bit width is `ColumnMeta[fieldIndex].Pallet.BitWidth`.
   - If multiple indices match → for each matching index, scan and union results (or fall back to strided scan with a multi-value check).

7. **Virtual ID field (IndexData).**  
   When the target field is the file's ID column and IDs are stored in a separate `IndexData` array rather than inline in records:
   - Encode search value as `int`.
   - Binary search or linear scan `section.IndexData` for the value.
   - On match, construct `RowHandle` directly. (Note: `TryGetRowHandle` already does this via a sorted index — the scanner can delegate to it for the single-value case, or do a vectorized scan of the `IndexData` span for the "all matches" case.)

8. **Sparse file handling.**  
   For files with `Db2Flags.Sparse`:
   - Records are variable-size; `pos / RecordSize` backtracking doesn't work.
   - Use strided scan (Step 4) exclusively, walking via `SparseRecordStartBits[]` and `SparseEntries[].Size` to find each record's start position.
   - Strings are inline in sparse mode; this scanner targets non-string fields so inline strings are out of scope.

9. **Encrypted section handling.**  
   Sections with non-zero `TactKeyLookup` already decrypt `RecordsData` in `Wdc5File`'s constructor when the key is available. If `RecordsData is null` (key unavailable), skip the section — same as current `EnumerateRowHandles()` behavior.

10. **Array fields.**  
    Fields with `ColumnMetaData.Size > 0` may have multiple elements per record. The scanner should check all array element offsets within the record (element `j` starts at `RecordOffset + j * elementBitWidth`). If any element matches, the row is a hit.

11. **Unit tests in [tests/MimironSQL.Formats.Wdc5.Tests/](tests/MimironSQL.Formats.Wdc5.Tests/).**  
    New test file `Db2/Wdc5RecordScannerTests.cs`:
    - Test byte-aligned scan finds correct rows for `int`, `uint`, `float`, `short`, `byte`, `double` values.
    - Test bit-packed scan finds correct rows for non-byte-aligned fields.
    - Test Common compression: value == default returns complement set; value != default scans dictionary.
    - Test Pallet: reverse lookup correctly maps to pallet indices.
    - Test false positives: a value appearing in a non-target column at the same byte pattern doesn't produce a result.
    - Test multi-section files.
    - Test sparse file fallback to strided scan.
    - Test empty result set (value not present).
    - Test array fields with multiple elements.
    - Verify results match the baseline: `EnumerateRowHandles().Where(h => file.ReadField<T>(h, fieldIndex).Equals(value))`.

12. **`InternalsVisibleTo` access.**  
    `Wdc5RecordScanner` is internal. The test project [tests/MimironSQL.Formats.Wdc5.Tests/](tests/MimironSQL.Formats.Wdc5.Tests/) already has `InternalsVisibleTo` access (per project convention). Verify this is in place; if not, add the attribute in an existing `InternalsVisibleTo.cs` or the `.csproj`.

13. **Future integration hook (do not implement now).**  
    Document in a code comment on `Wdc5RecordScanner` that the EF Core layer can eventually call this scanner when `Db2RowPredicateCompiler` detects a simple equality predicate on a single column, transforming the scan from `EnumerateRowHandles + ReadField per row` to `ScanFieldEquals` which yields only matching handles. This would slot into [Db2TableEnumerator.cs](src/MimironSQL.EntityFrameworkCore/Query/Internal/Db2TableEnumerator.cs) or its successor.

**Verification**

- `dotnet build MimironSQL.slnx` — no compilation errors.
- `dotnet test MimironSQL.slnx` — all existing tests pass; new `Wdc5RecordScannerTests` pass.
- Correctness baseline: for every test case, assert that `ScanFieldEquals<T>(fieldIndex, value, results)` produces the same set of `RowHandle`s as the naive `EnumerateRowHandles().Where(...)` approach.
- (Optional) Add a benchmark in [tests/MimironSQL.Benchmarks/](tests/MimironSQL.Benchmarks/) comparing `ScanFieldEquals` vs naive enumeration on a real DB2 file to quantify the speedup.

**Decisions**

- **Two-strategy dispatch**: Vectorized `IndexOf` for byte-aligned ≥ 3-byte fields (maximizes SIMD benefit); strided bit-scan for bit-packed or short fields (avoids false-positive churn).
- **Compression reverse-mapping**: Common → dictionary scan; Pallet → find matching indices then scan record data for those indices. No record data is searched for Common-compressed columns at all.
- **Scope**: Equality only for now. The strided scan trivially extends to comparison operators (`<`, `>`, range) in a follow-up — note this in the code.
- **Sparse files**: Strided scan only (no `IndexOf` backtracking since records are variable-size).
- **Placement**: Internal class in `MimironSQL.Formats.Wdc5`, not exposed through `IDb2File` — keeps the contract format-agnostic.
