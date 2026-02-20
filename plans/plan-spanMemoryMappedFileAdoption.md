## Plan: Span/Memory & MemoryMappedFile Adoption

Replace the `Stream`-based reading pipeline with `ReadOnlyMemory<byte>`-backed parsing throughout. Introduce `MemoryMappedFile` in provider projects (CASC primary, FileSystem secondary) so the format layer operates on memory-mapped or pre-decoded byte buffers, eliminating `BinaryReader`, `MemoryStream` wrappers, and numerous intermediate `byte[]` allocations. Remove the Lazy/Eager distinction in `Wdc5File` since slicing memory is zero-cost. These are breaking changes to the contract interfaces.

### Stage 1 — New Contracts & Types

Replace `IDb2StreamProvider` with a memory-oriented provider interface and update `IDb2Format` to accept `ReadOnlyMemory<byte>`.

#### Changes:

1. Delete `IDb2StreamProvider` in `MimironSQL.Contracts/Providers/IDb2StreamProvider.cs`.
2. Add `IDb2DataProvider` in `MimironSQL.Contracts/Providers/`:
   - `IDb2DataHandle OpenDb2(string tableName)` — synchronous (MMF access and BLTE decode are CPU-bound, no real async I/O)
   - `Task<IDb2DataHandle> OpenDb2Async(string tableName, CancellationToken)` — for future CDN/network providers
3. Add `IDb2DataHandle` in `MimironSQL.Contracts/Providers/`:
   - `ReadOnlyMemory<byte> Data { get; }` — the raw DB2 bytes
   - Implements `IDisposable` — for CASC: no-op (decoded `byte[]` is GC'd); for FileSystem: releases `MemoryMappedViewAccessor`
4. Update `IDb2Format` in `MimironSQL.Contracts/Formats/IDb2Format.cs`:
   - `OpenFile(Stream stream)` → `OpenFile(ReadOnlyMemory<byte> data)`
   - `GetLayout(Stream stream)` → `GetLayout(ReadOnlyMemory<byte> data)`
5. Update `MimironDb2Store` in `MimironSQL.EntityFrameworkCore/Storage/MimironDb2Store.cs`:
   - Depend on `IDb2DataProvider` instead of `IDb2StreamProvider`
   - Track `IDb2DataHandle` alongside `IDb2File` in `_fileCache` — dispose both on scope end
6. Update DI registrations in `MimironDb2ServiceCollectionExtensions.cs` and provider-specific extension methods to register `IDb2DataProvider` instead of `IDb2StreamProvider`.
7. Delete `Wdc5RecordLoadingMode` enum and `Wdc5FileOptions.RecordLoadingMode` from `Wdc5FileOptions.cs` — no longer needed.

#### Acceptance Criteria:
- [ ] `IDb2StreamProvider` no longer exists; all references compile against `IDb2DataProvider`
- [ ] `IDb2DataHandle` is `IDisposable` with `ReadOnlyMemory<byte> Data`
- [ ] `IDb2Format.OpenFile` and `GetLayout` accept `ReadOnlyMemory<byte>` instead of `Stream`
- [ ] `MimironDb2Store.Dispose()` disposes both `IDb2File` and `IDb2DataHandle` for every cached table
- [ ] `Wdc5RecordLoadingMode` enum is removed
- [ ] Solution compiles (test stubs updated to new signatures)

### Stage 2 — Wdc5File: Memory-Backed Parsing

Rewrite `Wdc5File` to parse from `ReadOnlyMemory<byte>` using `BinaryPrimitives` and span slicing, eliminating `BinaryReader` and all `ReadBytes()` allocations.

#### Changes:

1. Constructor accepts `ReadOnlyMemory<byte> data` instead of `Stream`.
2. Store `_data` as a field; parse header/sections via span + `BinaryPrimitives` using offsets.
3. Section record data and the dense string table become `ReadOnlyMemory<byte>` slices of `_data` (zero-copy).
4. Use `MemoryMarshal.Cast` on spans for struct arrays instead of `ReadBytes`.
5. Remove stream-based lazy read path: delete `GetRowBytesFromStream`, `_cachedRowBytes`, and related code.
6. `ReadField<T>` always reads from `section.RecordsData.Span`.
7. Remove `_reader`, `_stream` fields; adjust `Dispose()` to release `_data` reference only.

#### Acceptance Criteria:
- [ ] `Wdc5File` constructor accepts `ReadOnlyMemory<byte>` — no `Stream`, no `BinaryReader`
- [ ] Zero `new byte[]` allocations during header/metadata parsing (section data and string table are slices)
- [ ] `_stream`, `_reader`, `_cachedRowBytes`, `GetRowBytesFromStream`, `EnsureMaterializedForEnumeration` removed
- [ ] `ReadField<T>` always reads from `section.RecordsData.Span`
- [ ] `Wdc5LayoutReader.GetLayout` accepts `ReadOnlyMemory<byte>` instead of `Stream`
- [ ] `DecryptRowBytes` continues to use `ArrayPool` for writable buffers
- [ ] Unit tests covering header parsing, field reads, string table access, and encrypted section handling updated to new constructor

### Stage 3 — CASC Provider: MemoryMappedFile for Archives

Primary focus. Replace `FileStream` reads of CASC archive data files with `MemoryMappedFile`, and eliminate the `byte[] → MemoryStream` wrapping in the BLTE decode path.

#### Changes:

1. `CascLocalArchiveReader` caches `MemoryMappedFile` and `MemoryMappedViewAccessor` per archive.
2. Add a `MappedArchiveMemoryManager : MemoryManager<byte>` helper to expose `ReadOnlyMemory<byte>` from a view accessor safely.
3. Replace `ReadBlteBytesAsync` with `GetBlteData` returning `ReadOnlyMemory<byte>` slices pointing into the mapped view.
4. `CascDb2DataProvider.OpenDb2()` returns `IDb2DataHandle` wrapping the decoded DB2 bytes (no `MemoryStream`).
5. `BlteDecoder` operates on `ReadOnlySpan<byte>` and decodes into pooled buffers when possible.
6. Dispose patterns to release view accessors and MMFs when appropriate.

#### Acceptance Criteria:
- [ ] `CascLocalArchiveReader.GetBlteData(ekey)` returns `ReadOnlyMemory<byte>` from memory-mapped archive data — no `FileStream`, no `new byte[blteSize]` allocation
- [ ] Archive `MemoryMappedFile`s are cached and reused across calls
- [ ] `CascDb2DataProvider.OpenDb2()` returns `IDb2DataHandle` with decoded DB2 bytes — no `MemoryStream`
- [ ] BLTE signature scanning operates on the mapped span without allocating a prefix buffer
- [ ] `CascLocalArchiveReader.Dispose()` releases all `MemoryMappedViewAccessor`s and `MemoryMappedFile`s

### Stage 4 — FileSystem Provider: MemoryMappedFile for DB2 Files

Secondary priority. Replace `File.OpenRead` + `FileStream` with memory-mapped DB2 file access.

#### Changes:

1. Implement `MappedMemoryManager` utility wrapping `MemoryMappedViewAccessor`.
2. `FileSystemDb2DataProvider.OpenDb2()` memory-maps the `.db2` file, returns an `IDb2DataHandle` that owns the mapping and exposes `ReadOnlyMemory<byte>`.
3. Cache MMFs per table name, dispose on provider shutdown.

#### Acceptance Criteria:
- [ ] `FileSystemDb2DataProvider.OpenDb2()` returns `IDb2DataHandle` backed by `MemoryMappedFile` — no `FileStream`
- [ ] DB2 files are memory-mapped read-only
- [ ] `IDb2DataHandle.Dispose()` properly releases `MemoryMappedViewAccessor` and `MemoryMappedFile`

### Stage 5 — Allocation Reduction Pass

Targeted span/memory optimizations in hot-path code that weren't covered by the structural refactoring above.

#### Changes:

1. `Wdc5RowReader.ReadCString()` → use `IndexOf((byte)0)` and slice, zero allocations.
2. `BlteDecoder` → use `ArrayPool` for decryption/decompression intermediates; avoid `payload.ToArray()` for `N` blocks.
3. `CascEncodingIndex` → avoid `ToArray()`; accept `ReadOnlyMemory<byte>` or parse in-place without retaining full copy.
4. Use `FrozenDictionary` for `_idIndex` and pre-size copy maps.

#### Acceptance Criteria:
- [ ] `Wdc5RowReader.ReadCString()` allocates zero intermediate buffers
- [ ] `BlteDecoder.Decode()` uses `ArrayPool` for intermediate buffers
- [ ] No `payload.ToArray()` in uncompressed 'N' block handling
- [ ] `_idIndex` is `FrozenDictionary` after construction

## Verification

- `dotnet build MimironSQL.slnx` compiles after each stage
- `dotnet test MimironSQL.slnx` — tests may be adjusted for interface changes; core logic validated
- Run `MimironSQL.Benchmarks` (especially `BlteDecodeBenchmarks`) after Stage 5 to measure allocation reduction
- Manually open a large DB2 (e.g., Spell table from CASC) to confirm behavior
- Confirm memory-mapped files are disposed and no handles leak after `DbContext.Dispose()`

## Decisions

- Replace `IDb2StreamProvider` with `IDb2DataProvider` (breaking change)
- Always-eager parsing: remove `RecordLoadingMode`
- Provider owns MMF; format layer remains MMF-agnostic
- `IDb2DataHandle` is disposable and the canonical owner of the returned memory
- CASC is primary; FileSystem MMF is secondary
- MMF access is synchronous; `IDb2DataProvider` retains async overload for future network providers


