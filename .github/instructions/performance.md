# Performance Guidelines

## General
- Internally prefer high performance data structures such as `Array` over `List`. Public APIs should prefer `List`, `IEnumerable`, `IList`, `ICollection`, etc, over `Array`.

## Important Concepts
These are most important in hot paths (DB2 parsing/decoding, query execution, expression translation).

### Data structures

- Choose data structures based on access patterns (indexing vs lookups vs iteration).
- Prefer `Array` for tight loops and fixed-size data; prefer `Dictionary<TKey, TValue>` for frequent key lookups.
- Avoid unnecessary layering (e.g., converting between `Array`/`List` repeatedly) unless it improves API clarity.

### Minimize allocations

- Treat allocations in inner loops as suspect; prefer reuse (buffers, builders, temporary collections) when it stays readable.
- Prefer value types for small, immutable “data bag” types when it reduces GC pressure.
- If pooling is used, prefer BCL pools (e.g., `ArrayPool<T>`) and ensure rented buffers are always returned.

### Memory usage, `Span<T>`, `Memory<T>`

- Prefer `ReadOnlySpan<byte>` / `ReadOnlySpan<char>` for parsing/decoding when you can stay within the caller’s stack-lifetime.
- Use `ReadOnlyMemory<T>` when data must outlive a single stack frame (e.g., async boundaries).
- Prefer slicing (`span.Slice(...)`) over copying (`ToArray`, `Substring`) in parsing/decoding paths.

### Profile before micro-optimizing

- Keep code clean and maintainable by default; optimize only when a bottleneck is identified.
- When introducing “clever” performance code, prefer a small benchmark and/or before/after profiling notes.

### Async where it helps

- Prefer `async`/`await` at I/O boundaries (filesystem, CASC, network) to avoid blocking threads.
- Avoid async overhead inside CPU-bound inner loops; keep parsing/decoding paths synchronous unless there’s real parallel I/O.

### Avoid boxing/unboxing

- Prefer generics and strongly-typed APIs; avoid using `object`, non-generic collections, or interface-typed values in hot paths when it causes boxing.
- Be careful with value types flowing through `IEnumerable`, `IComparable`, `IFormattable`, or string interpolation patterns that box.

### Prefer immutable semantics for shared state

- Prefer immutable semantics for shared metadata/config (e.g., schema/model objects): construct once, then treat as read-only.
- Prefer exposing read-only views (`IReadOnlyList<T>`, `IReadOnlyDictionary<TKey, TValue>`) over mutable collections.