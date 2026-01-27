# Coding style (project-specific)

These rules are **in addition** to the general guidance in `.github/copilot-instructions.md`.

All rules in this document are **guidelines**. Use judgment, and prioritize clarity unless profiling/benchmarks show a real need.

## General
- Prefer primary constructors where it reduces boilerplate.
- Prefer collection expressions and spread over `ToArray()` / `ToList()` / manual loops when building collections:
    - Prefer `[.. someEnumerable]` over `someEnumerable.ToArray()`.
    - Prefer `var list = someEnumerable.ToList()` over `List<T> list = [.. someEnumerable];` (i.e. variable declaration and assignment)
    - Prefer `List<T> list = [item1, item2];` and `Dictionary<TKey, TValue> dict = [];` (i.e. property declarations)
    - Exception: interoperability/compatibility requirements with third-party APIs.
- Internally prefer high performance data structures such as `Array` over `List`. Public APIs should prefer `List`, `IEnumerable`, `IList`, `ICollection`, etc, over `Array`.

## Performance (Guidelines)

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

## Avoid static offloading
- Avoid offloading work into `static` helper methods as a way to avoid constructor logic or to emulate factory patterns (e.g., `static Load(...)`, `static Create(...)`, `static From(...)`, etc.).
- Prefer:
  - expression-bodied field/property initialization,
  - instance methods,
  - non-static local functions (when helpful),
  - straightforward constructor code when it’s the natural place for initialization.

Rationale: we already avoid static factory-like constructors; `static` “load/build” helpers are effectively the same pattern.

## No discard parameters in signatures

- Avoid discards (`_`) in method signatures.
- Prefer that every parameter in a method signature is used by the method body.
- Discards are fine at **call sites** (e.g., `TryGetValue(key, out _)`) or similar scenarios where an API requires it.

## Prefer natural exceptions over defensive validation

- Avoid overly defensive guards for clearly-invalid inputs/configuration (e.g., null options, null/empty/incorrect file paths, malformed input files).
- Assume configuration/data is correct; if it’s not, let exceptions naturally surface from the BCL/APIs (e.g., `File.ReadAllLines`, `ulong.Parse`, `Convert.FromHexString`).
- Avoid catch/rethrow wrappers that “validate” or rewrite parsing errors; they tend to obscure the original exception.

## Pattern Matching

Goal: keep pattern matching usage consistent and readable.

### Do

- Prefer **property patterns** when a *property/member is being compared to a constant*.
  - Examples (OK):
    - `bytes is { Length: 0 }`
    - `bytes is { Length: < 4 }`
    - `activeBuilds is { Count: not 0 }`
    - `section is { TactKeyLookup: not 0 }`
    - `columnMeta is { Pallet.Cardinality: 1 }`

### Don’t

- Do not rewrite non-property comparisons into constant/relational patterns.
  - Examples (Bad → keep operators):
    - `x is 0` / `x is not 0` → prefer `x == 0` / `x != 0`
    - `x is < 0` / `x is >= 0` → prefer `x < 0` / `x >= 0`

- Do not use nested property patterns.
  - Examples:
    - OK: `columnMeta is { Pallet.Cardinality: 1 }`
    - Bad: `columnMeta is { Pallet: { Cardinality: 1 } }`

- Do not use `typeof(...)` inside property patterns (C# limitation; triggers CS9135).
  - Example (Bad): `node is { Method.DeclaringType: typeof(string) }`
  - Prefer: `node.Method.DeclaringType == typeof(string)`
