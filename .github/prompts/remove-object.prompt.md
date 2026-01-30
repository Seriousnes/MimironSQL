---
description: Remove `object` usage from a specified production file, replacing it with generics/interfaces/types while keeping the refactor scoped and verifiable.
agent: agent
---

# Remove `object`/`System.Object` from a specified file

You are working in the **MimironSQL** repository.

## Hard constraints (project + session)

- **Do not modify files outside** the MimironSQL repository.
- **Goal:** eliminate the `object` keyword from the **specified target file** by replacing it with generics, interfaces, and types.
- **Production exception:** the only acceptable `object` in production code is where it is **unavoidable due to framework-mandated signatures** (e.g., interfaces that require `object`).
- **Scope control:** keep changes to **one file at a time** unless additional edits are strictly required to keep the code compiling/tests passing.
- Prefer minimal diffs and avoid unrelated refactors.
- Use modern C# (target .NET 10 / C# 14) and follow the repo coding/performance guidance.

## No-questions defaults (proceed without Q&A)

Do not ask the user any questions. Proceed with these defaults:

- **Target file selection:**
  - If the user provides a file path, use it.
  - Otherwise, use the **currently active editor file** as the target.
- **Scope:** refactor the target file only. If compilation/tests fail and require changes elsewhere, make the smallest required edits, and ensure you do not introduce new `object` usage in any additional production files you touch.
- **Exceptions:** framework-mandated `object` signatures are allowed only where strictly required by the framework/interface contract.
- **Casts:** eliminate any usage that includes the `object` keyword, including casts like `(T)(object)`, within the target file.
- **Design latitude:** you may rely on existing interfaces/base types (e.g., `IDb2Table`) and you may add internal helper types/methods if needed.
- **Reflection:** replace `Invoke`/runtime reflection object plumbing with cached typed delegates when it removes `object`.
- **Verification:** run all tests (`dotnet test`) after the change.

## Execution checklist (do the work)

### 1) Inventory the `object` usage

- Identify every `object` in the target file:
  - `object`-typed fields/properties
  - `Func<..., object>` / `Action<..., object>` delegates
  - `Expression.Parameter(typeof(object))`
  - `MethodInfo.Invoke` returning `object`
  - helper methods returning `object`
  - casts like `(T)(object)`

### 2) Replace with typed alternatives

Use the smallest correct replacement:

- **Heterogeneous caching** → prefer a shared interface/base type.
  - Example pattern: `ConcurrentDictionary<Type, Func<Ctx, string, IFoo>>`
- **Reflection invoke to create generic types** → replace with cached typed delegates.
  - Prefer: `CreateDelegate(...)` over `Invoke(...)`
  - If runtime generic arguments vary, key the cache by `(entityType, rowType)`.
- **Property setters** → compile typed setters using expression trees with interface/base parameter (not `object`).
- **Avoid `dynamic`** (it’s effectively object-like and can hide issues).

### 3) Keep scope tight

- Do not reformat or rename unrelated identifiers.
- Do not change behavior; only strengthen typing.
- If a supporting type is required, prefer using an **existing** interface/base type.

### 4) Validate

- Ensure the **target file contains zero `object` keyword usage** (unless allowed exception applies).
- Compile and run tests.
- If failures occur, fix only what is necessary and remain within scope rules.

## Acceptance criteria

- The target file contains **no `object` keyword** (except explicitly allowed framework-mandated signatures).
- `MethodInfo.Invoke`/reflection-based creation paths are replaced with **typed delegates** where that eliminates `object`.
- The project builds and the chosen tests pass.
- Changes remain minimal and focused.
