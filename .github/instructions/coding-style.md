# Coding style (project-specific)

These rules are **in addition** to the general guidance in `.github/copilot-instructions.md`.

## Avoid static offloading (Hard)

- Do **not** offload work into `static` helper methods as a way to avoid constructor logic or to emulate static factory patterns (e.g., `static Load(...)`, `static Create(...)`, `static From(...)`, etc.).
- Prefer:
  - expression-bodied field/property initialization,
  - instance methods,
  - non-static local functions (when helpful),
  - straightforward constructor code when it’s the natural place for initialization.

Rationale: we already avoid static factory-like constructors; `static` “load/build” helpers are effectively the same pattern.

## No discard parameters in signatures (Hard)

- Discards (`_`) are **forbidden** in method signatures.
- Every parameter in a method signature must be used by the method body.
- Discards are fine at **call sites** (e.g., `TryGetValue(key, out _)`) or similar scenarios where an API requires it.

## Prefer natural exceptions over defensive validation

- Avoid overly defensive guards for clearly-invalid inputs/configuration (e.g., null options, null/empty/incorrect file paths, malformed input files).
- Assume configuration/data is correct; if it’s not, let exceptions naturally surface from the BCL/APIs (e.g., `File.ReadAllLines`, `ulong.Parse`, `Convert.FromHexString`).
- Avoid catch/rethrow wrappers that “validate” or rewrite parsing errors; they tend to obscure the original exception.

## Pattern Matching (Hard)

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
