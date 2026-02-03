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

## Avoid static offloading
- Avoid offloading work into factory-like `static` methods as a way to avoid constructor logic or to emulate factory patterns (e.g., `static Load(...)`, `static Create(...)`, `static From(...)`, etc.).
- Prefer:
  - expression-bodied field/property initialization,
  - instance methods,
  - non-static local functions (when helpful),
  - straightforward constructor code when it’s the natural place for initialization.

Notes:
- This guideline is about factory/creation flows and "static builder" APIs, not about all `static` methods.
- Small, well-scoped `static` helpers and extension methods are fine when they reduce duplication and keep call sites readable.

Rationale: we already avoid static factory-like constructors; `static` “load/build” helpers are effectively the same pattern.

## Loops Patterns

### Implicit Loop filters
❌ BAD
```csharp
foreach (var i in type.GetInterfaces())
{
    if (!i.IsGenericType)
        continue;

    if (i.GetGenericTypeDefinition() != typeof(IEnumerable<>))
        continue;

    elementType = i.GetGenericArguments()[0];
    return true;
}
```

✅ GOOD
```csharp
foreach (var i in type.GetInterfaces().Where(x => !x.IsGenericType && i.GetGenericTypeDefinition() != typeof(IEnumerable<>)))
{
    elementType = i.GetGenericArguments()[0];
    return true;
}
```

### Mapping iteration variable
❌ BAD
```csharp
foreach (var part in payload.Split([','], StringSplitOptions.RemoveEmptyEntries))
{
    var token = part.Trim();
    if (token.Length == 0)
        continue;

    if (uint.TryParse(token, System.Globalization.NumberStyles.HexNumber, provider: null, out var hash))
        currentLayoutHashes.Add(hash);
}
```

✅ GOOD
```csharp
foreach (var part in payload.Split([','], StringSplitOptions.RemoveEmptyEntries).Select(p => p.Trim()).Where(t => t is { Length: > 0 }))
{
    if (uint.TryParse(token, System.Globalization.NumberStyles.HexNumber, provider: null, out var hash))
        currentLayoutHashes.Add(hash);
}
```

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

### Inline null-check + assignment

- Prefer inline pattern matching for “get value and null-check” flows, instead of assigning a local then checking it.
- If the matched value is **not used** in the block, use a discard designation (`_`).
- If the matched value **is used** in the block, use a meaningful variable name.

❌ BAD
```csharp
var ctor = type.GetConstructor(Type.EmptyTypes);
if (ctor is not null)
{
    instance = (IDb2Format)Activator.CreateInstance(type)!;
    return true;
}
```

✅ GOOD (value not used)
```csharp
if (type.GetConstructor(Type.EmptyTypes) is { } _)
{
    instance = (IDb2Format)Activator.CreateInstance(type)!;
    return true;
}
```

✅ GOOD (value used)
```csharp
if (type.GetConstructor(Type.EmptyTypes) is { } ctor)
{
    instance = (IDb2Format)ctor.Invoke(parameters: null)!;
    return true;
}
```

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