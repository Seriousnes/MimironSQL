## Plan: Wdc5FieldAccessor — Slim Field Access + Sparse Offset Table

### Overview

Two complementary components inside `src/MimironSQL.Formats.Wdc5/Db2/`:

1. **`Wdc5FieldAccessor`** — a pre-resolved, per-section "fast-path" that reads a single scalar field from a dense record in ~4 array lookups + 1 compression switch + 1 bit-read, bypassing the 20+ branches and 2 dictionary probes that the full `ReadField<T>` call chain performs per row.

2. **`Wdc5SparseOffsetTable`** — a per-section, eagerly-built table of `(rowIndex, fieldIndex) → bitPosition` for sparse sections with inline strings, eliminating the O(N) `SkipSparseField` loop that currently walks all preceding fields on every read.

Together they give the format provider a "column-jump" capability: for any field in any row, the reader can position itself in O(1) without sequential field decoding. This pairs naturally with either the B+-tree index plan (post-lookup field reads) or the record scanner plan (tight single-column scan loops) for non-indexed columns.

---

### What `ReadField<T>` currently does (and what's avoidable)

Per the research, `ReadField<T>` performs these steps on **every call**:

| Step | What | Avoidable? |
|------|------|------------|
| Bounds-check section index | Branch | Yes — pre-validate once |
| Fetch section from `List<T>` | Indexer | Yes — cache ref outside loop |
| Bounds-check row index | Branch | Yes — scanner validates range |
| Null-check `RecordsData` | Branch | Yes — known at open time |
| `CreateReaderAtRowStart` | Sparse/dense branch | Yes — known at open time |
| `GetVirtualId` | Array read or field loop | Avoidable for non-Common scalars |
| `EncryptedRowNonceStrategy` | Enum compare | Yes — only for encrypted sections |
| `globalRowIndex` calculation | Addition | Avoidable for non-string scalars |
| `SecondaryKey` + `IndexData` check | Flag + null check | Yes — pre-validate once |
| `ParentLookupEntries.TryGetValue` | Dictionary probe | Yes — only for parent virtual field |
| `IsDecryptable` check + Salsa20 | Branch + potential decrypt | Yes — ~99% sections are plaintext |
| `typeof(T).IsEnum` | Reflection (JIT-dead) | JIT-eliminated |
| `fieldIndex < 0` virtual field | Branch | Yes — fast path knows physical |
| `typeof(T) == typeof(string)` | Branch (JIT-dead) | JIT-eliminated |
| `type.IsArray` | Reflection (JIT-dead) | JIT-eliminated |
| `MoveToFieldStart` | 1 array read + 1 add | **Always needed** |
| 4× metadata lookups | Array indexers | **Always needed** |
| `ReadScalarTyped` type chain | 10 branches (JIT-dead except 1) | JIT-eliminated |
| Compression switch + bit read | Switch + arithmetic | **Always needed** |

**Irreducible minimum**: `MoveToFieldStart` (1 array index + 1 add) + 4 metadata lookups + 1 compression switch + 1 bit-read.

---

### Steps

#### Part A: `Wdc5FieldAccessor` (Dense Fast Path)

1. **Create `src/MimironSQL.Formats.Wdc5/Db2/Wdc5FieldAccessor.cs`.**

   Internal `ref struct` that pre-captures everything the hot loop needs:

   - **Construction** (once per section × field scan): takes `Wdc5File`, `Wdc5Section`, `int fieldIndex`. Validates once:
     - `RecordsData is not null`
     - Not sparse (dense accessor; sparse variant in Part B)
     - Not encrypted (`!section.IsDecryptable`)
     - `fieldIndex >= 0 && fieldIndex < Header.FieldsCount`
     - Pre-resolves: `recordSize`, `recordsSpan`, `fieldMeta`, `columnMeta`, `palletData`, `commonData`, `bitOffset = columnMeta.RecordOffset`.
     - For `Common` compression: also pre-capture `section.IndexData` ref (needed to resolve source ID for dictionary lookup).

   - **`ReadScalar<T>(int rowIndex) → T`** (per-row hot path):
     Computes `rowStartBits = rowIndex * recordSize * 8`, adds `bitOffset`, constructs `Wdc5RowReader` at that position, calls `Wdc5FieldDecoder.ReadScalar<T>`. For non-`Common` columns, passes `sourceId = 0` (unused by decoder). For `Common` columns, resolves `sourceId` from `IndexData[rowIndex]`.

   - **`ReadScalarRaw(int rowIndex) → ulong`** (for scanner/comparator use):
     Same positioning but returns raw `ulong` from `ReadUInt64(bitWidth)` without type conversion. Useful for the record scanner plan where the caller pre-encodes the search value.

   Per-row cost: **1 multiply + 1 add + 1 `Wdc5RowReader` ctor + 1 `ReadScalar` call** (1 compression switch + 1 `ReadUInt64`).

2. **Encrypted section support.**
   Construction throws if `section.IsDecryptable`. Callers fall back to `ReadField<T>` for encrypted sections. Encrypted sections are rare (~1-3 tables per WoW build) and Salsa20 nonce-per-row makes batching difficult.

3. **String field support.**
   Not supported directly. Dense strings are a fixed-size int offset — the accessor could expose `ReadStringOffset(rowIndex) → int` later; full string resolution stays in `ReadField<string>`.

4. **Integration with `Wdc5File`.**
   Add a method: `internal Wdc5FieldAccessor CreateFieldAccessor(int sectionIndex, int fieldIndex)` that constructs and returns an accessor. Callers (scanner, enumerator) use this in their hot loop instead of `ReadField<T>`.

#### Part B: `Wdc5SparseOffsetTable` (Sparse Offset Cache)

5. **Create `src/MimironSQL.Formats.Wdc5/Db2/Wdc5SparseOffsetTable.cs`.**

   Internal class that pre-computes column bit-positions for every row in a sparse section:

   - **Storage**: `int[]` of size `numRows × numFields`. Access: `table[rowIndex * numFields + fieldIndex]`.
   - **Construction**: walks every row once, calling `SkipSparseField` for each field, recording the bit position after each skip. O(rows × fields) — same as reading every field once — but done eagerly once per section.
   - **Lookup**: `GetFieldBitPosition(int rowIndex, int fieldIndex) → int` — single array indexer, O(1).

6. **Memory budget.**
   10K rows × 30 fields × 4 bytes = ~1.2 MB per sparse section. Largest known sparse WoW tables (~50K rows): ~6 MB. Acceptable as a one-time cost per loaded sparse section.

7. **Lifecycle — eager vs lazy.**
   Controlled by service builder configuration:

   - Add a property to `Wdc5FormatOptions` (or `Wdc5FileOptions`): `bool EagerSparseOffsetTable` (default `false`).
   - **Eager** (`true`): built during `Wdc5File` construction for every sparse section.
   - **Lazy** (`false`): built on first field access in a sparse section. Stored on `Wdc5Section` as `Lazy<Wdc5SparseOffsetTable>`.

8. **Integration with sparse reading paths.**
   Modify `TryGetInlineString` and any sparse scalar-reading path:
   - If offset table available: `reader.PositionBits = table.GetFieldBitPosition(rowIndex, fieldIndex)` — skips the `SkipSparseField` loop.
   - If not available: fall through to existing loop (backward-compatible).

9. **`Wdc5SparseFieldAccessor`** — sparse counterpart to `Wdc5FieldAccessor`.
   Uses the offset table for O(1) positioning, then calls `Wdc5FieldDecoder.ReadScalar<T>` directly. Same minimal overhead as the dense accessor.

#### Part C: Service Configuration

10. **Create or extend format options.**
    Check if `Wdc5FileOptions` exists. Add `bool EagerSparseOffsetTable`. Wire through DI as a singleton `Wdc5FormatOptions` injected into `Wdc5Format`.

11. **Builder extension.**
    Expose via the provider builder chain as an extension on `IMimironDb2DbContextOptionsBuilder` or `MimironDb2DbContextOptionsBuilder`, e.g.: `.ConfigureWdc5(o => o.EagerSparseOffsetTable = true)`.

#### Part D: Tests

12. **`Wdc5FieldAccessorTests`** in `tests/MimironSQL.Formats.Wdc5.Tests/`.
    - `ReadScalar<T>` matches `ReadField<T>` for all rows, all numeric types.
    - All compression types: `None`, `Immediate`, `SignedImmediate`, `Common`, `Pallet`.
    - Construction throws for encrypted sections.
    - Construction throws for string fields.
    - `ReadScalarRaw` returns expected raw encoding.

13. **`Wdc5SparseOffsetTableTests`** in `tests/MimironSQL.Formats.Wdc5.Tests/`.
    - All field positions match `SkipSparseField` loop.
    - Field reads via offset table produce same values as `ReadField<T>`.
    - Memory size within expected bounds.
    - Lazy vs eager construction behavior.

14. **`InternalsVisibleTo`** — verify/add for test project access.

---

### How This Pairs with the Other Plans

| Plan | Benefit |
|------|---------|
| **B+-tree index** | After index returns matching `RowHandle`s, post-lookup field reads use `Wdc5FieldAccessor` instead of `ReadField<T>` — ~15 fewer branches per field per row. |
| **Record scanner** | Scanner's strided bit-scan is exactly `Wdc5FieldAccessor.ReadScalarRaw` — formalizes it as reusable. Adds `Common`/`Pallet` support. |
| **Non-indexed queries** | Full-table scans with row predicates use the accessor for cheaper per-row evaluation. |
| **Sparse file queries** | `Wdc5SparseOffsetTable` makes any sparse access O(1) per field. |

---

### Decisions

- **`ref struct` for `Wdc5FieldAccessor`**: used within scan loop scope. Promote to class later if storage in a field is needed.
- **No encrypted section support**: fall back to `ReadField<T>`. Optimize only if profiling warrants.
- **No string field support**: dense strings are fixed-size int offsets; full resolution stays in `ReadField<string>`. Future `ReadStringOffset` method possible.
- **Eager sparse table is opt-in**: default lazy (built on first access). Eager for users who know they'll touch many sparse columns.
- **`ReadScalarRaw` for scanner integration**: returns `ulong` to avoid generic dispatch overhead.

---

### Verification

- `dotnet build MimironSQL.slnx` — compilation succeeds.
- `dotnet test MimironSQL.slnx` — all existing + new tests pass.
- Correctness: `accessor.ReadScalar<T>(rowIndex) == file.ReadField<T>(handle, fieldIndex)` for all rows.
- (Optional) Benchmark: `Wdc5FieldAccessor` loop vs `ReadField<T>` loop over 10K rows — expect measurable reduction in branch mispredictions and instruction count.
