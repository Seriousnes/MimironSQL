## Plan: Remaining Gaps — Custom Indexes And Field Accessor Coverage

### Current Status

- `Db2IndexFileFormat`, `Db2IndexWriter`, `Db2IndexReader`, `Db2IndexValueEncoder`, `Db2IndexBuilder`, `Db2IndexLookup`, and `WithCustomIndexes()` are implemented.
- `Wdc5FieldAccessor`, `Wdc5SparseOffsetTable`, `Wdc5SparseFieldAccessor`, `ConfigureWdc5(...)`, and the sparse offset-table lifecycle are implemented.
- `Wdc5RecordScanner` is implemented and covered for dense immediate, sparse immediate, common compression, pallet compression, duplicate-pallet reverse lookup, pallet-array matching, virtual-ID scans, false-positive rejection, and multi-section scans.

### Remaining Gaps

#### 1. Cover The Index Lifecycle More Completely

The reader/writer/lookup stack is in place, but the remaining lifecycle behavior is still under-tested.

Work:

1. Add a staleness test for layout-hash mismatch causing rebuild.
2. Add a staleness test for WoW-version mismatch causing rebuild.
3. Add a fallback test for corrupt or unreadable index files.
4. Decide whether to wire range predicates into the query pipeline or remove/defer the currently unused `TryFindRange(...)` path until predicate hinting supports it.

#### 2. Harden Field Accessor Validation Coverage

The field accessor work is effectively implemented, but a few validation scenarios remain thinly covered.

Work:

1. Add tests covering `Common`, `Pallet`, and `SignedImmediate` scalar reads through `Wdc5FieldAccessor` / `Wdc5SparseFieldAccessor`.
2. Add an explicit encrypted-section rejection test.
3. Decide whether unsupported string-field semantics should be documented as a schema-level concern or guarded earlier by higher-level callers.

### Verification

1. `dotnet test MimironSQL.slnx --filter "FullyQualifiedName~CustomIndexQueryTests|FullyQualifiedName~Wdc5FieldAccessorTests|FullyQualifiedName~Wdc5RecordScannerTests|FullyQualifiedName~Db2IndexTests"`
2. Confirm that the custom-index tests create `*.db2idx` files when expected.
3. Confirm that scanner and accessor tests cover all intended fast paths and fallback paths.