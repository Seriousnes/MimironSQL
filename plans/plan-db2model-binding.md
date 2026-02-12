# Plan: Replace `Db2Model` / `Db2ModelBuilder` with `Db2ModelBinding`

## Goal

Remove the entire custom `Db2Model` + `Db2ModelBuilder` layer and replace it with a new internal service called `Db2ModelBinding`.
`Db2ModelBinding` reads EF Core's runtime `IModel` directly, resolves DB2-specific metadata on demand (`.dbd` schema, case-insensitive field lookup, FK-array semantics), and caches results per entity type / navigation.

The `Db2ModelBuilder` fluent API (and its supporting metadata types) exist solely because the provider once needed a parallel model to EF Core's.
Now that the provider reads all relationship and mapping information from `IModel`, the translation layer (`MimironDb2Db2ModelProvider`) and builder graph are redundant overhead.

---

## Definitions

| Term | Meaning |
|---|---|
| **Db2ModelBinding** | The **new** service that replaces `Db2Model`. An adapter/cache over EF Core `IModel` + `.dbd` schema. |
| **Db2Model** | The **current** runtime model to be deleted. |
| **Db2ModelBuilder** | The **current** fluent builder to be deleted (along with all partial files). |
| **MimironDb2Db2ModelProvider** | The **current** translation layer (`IModel → Db2ModelBuilder → Db2Model`). To be replaced by `Db2ModelBinding`. |

---

## Architecture Overview

### Current Flow (to be removed)

```
EF IModel
    ↓ (MimironDb2Db2ModelProvider walks via reflection)
Db2ModelBuilder + Db2EntityTypeBuilder + Db2NavigationMetadata + ...
    ↓ Build() / BuildLazy()
Db2Model (runtime dictionaries of Db2EntityType, Db2ReferenceNavigation, Db2CollectionNavigation)
    ↓
Query pipeline consumers (Db2IncludeChainExecutor, Db2NavigationQueryCompiler, Db2QueryProvider, etc.)
```

### New Flow

```
EF IModel   ──┐
              ├──→  Db2ModelBinding (adapter/cache)
.dbd Schema ──┘         │
                        ↓
              Query pipeline consumers (same Db2EntityType, Db2ReferenceNavigation, Db2CollectionNavigation runtime types)
```

`Db2ModelBinding` is the **only** new type. It exposes the same lookup API that `Db2Model` does today, but builds each entry lazily from EF metadata + schema, with no intermediate builder.

---

## Phasing

### Phase 0 — Preparation

1. Add `[InternalsVisibleTo]` from the `MimironSQL.EntityFrameworkCore` project to any test project that does not already have it (if needed).
2. Read the `Db2ModelBuildMode` enum and the `MimironDb2OptionsExtension.Db2ModelBuildMode` property to decide how/whether `Db2ModelBinding` still offers lazy vs eager modes.
   - **Decision**: `Db2ModelBinding` will always resolve lazily (schema fetched on first access per entity type). The `Db2ModelBuildMode` enum and its option property are deleted.

### Phase 1 — Create `Db2ModelBinding`

**New file:** `src/MimironSQL.EntityFrameworkCore/Db2/Model/Db2ModelBinding.cs`

#### Public API surface (internal)

```csharp
internal sealed class Db2ModelBinding
{
    // Constructor
    Db2ModelBinding(IModel efModel, Func<string, Db2TableSchema> schemaResolver);

    // Entity type lookups (same as Db2Model)
    Db2EntityType GetEntityType(Type clrType);

    // Navigation lookups (same as Db2Model)
    bool TryGetReferenceNavigation(Type sourceClrType, MemberInfo navigationMember, out Db2ReferenceNavigation navigation);
    bool TryGetCollectionNavigation(Type sourceClrType, MemberInfo navigationMember, out Db2CollectionNavigation navigation);

    // Auto-include discovery (same as Db2Model)
    IReadOnlyList<MemberInfo> GetAutoIncludeNavigations(Type sourceClrType);
}
```

#### Internal implementation strategy

- **Entity types**: For each `IEntityType` in `IModel`, lazily build a `Db2EntityType` on first access. Cache in a `ConcurrentDictionary<Type, Lazy<Db2EntityType>>`.
  - Table name: `IEntityType.GetTableName() ?? clrType.Name`
  - Primary key member: `IEntityType.FindPrimaryKey()?.Properties[0].PropertyInfo`
  - Column name mappings: walk `IEntityType.GetProperties()`, call `property.GetColumnName()`, skip shadow properties, skip `[Column]`-attributed properties (they self-resolve), skip `Id`.
  - Schema: call `schemaResolver(tableName)` — this is the only `.dbd`-specific resolution.
  - PK field schema: `schema.Fields.First(f => f.IsId)`.

- **Reference navigations**: For each `INavigation` where `!IsCollection`, build a `Db2ReferenceNavigation` from the EF `IForeignKey`. Cache in `ConcurrentDictionary<(Type, MemberInfo), Lazy<Db2ReferenceNavigation>>`.
  - If `fk.IsUnique && fk.DeclaringEntityType == target && dependent PK == FK` → `SharedPrimaryKeyOneToOne`.
  - Otherwise → `ForeignKeyToPrimaryKey`.

- **Collection navigations**: For each `INavigation` where `IsCollection`, build a `Db2CollectionNavigation` from the EF `IForeignKey`. Cache in `ConcurrentDictionary<(Type, MemberInfo), Lazy<Db2CollectionNavigation>>`.
  - If there is a `[ForeignKey]` array property on the source (FK-array pattern) → `ForeignKeyArrayToPrimaryKey`.
  - Otherwise → `DependentForeignKeyToPrimaryKey`.

- **Auto-includes**: Walk `IEntityType.GetNavigations()`, filter `IsEagerLoaded`. Cache in `Dictionary<Type, IReadOnlyList<MemberInfo>>`.

- **Validation logic** from `Db2ModelBuilder` (e.g., `ValidateCollectionNavigationMemberType`, `IsIntKeyEnumerableType`, `HasAnyColumnMapping`) is ported into private methods of `Db2ModelBinding`.

### Phase 2 — Create `IDb2ModelBinding` interface and provider

**New file:** `src/MimironSQL.EntityFrameworkCore/Db2/Model/IDb2ModelBinding.cs`

```csharp
internal interface IDb2ModelBinding
{
    Db2ModelBinding GetBinding();
}
```

**New file:** `src/MimironSQL.EntityFrameworkCore/Db2/Model/Db2ModelBindingProvider.cs`

Replaces `MimironDb2Db2ModelProvider`. This is the DI-registered scoped service.

```csharp
internal sealed class Db2ModelBindingProvider(
    ICurrentDbContext currentDbContext,
    IMimironDb2Store store) : IDb2ModelBinding
{
    private Db2ModelBinding? _binding;

    public Db2ModelBinding GetBinding()
        => _binding ??= new Db2ModelBinding(
            _context.Model,
            tableName => store.GetSchema(tableName));
}
```

### Phase 3 — Update consumers

Every source file that references `Db2Model` or `IMimironDb2Db2ModelProvider` is updated to use `Db2ModelBinding` / `IDb2ModelBinding`.

| File | Change |
|---|---|
| `MimironDb2ServiceCollectionExtensions.cs` | Replace `IMimironDb2Db2ModelProvider` → `IDb2ModelBinding` registration |
| `MimironDb2QueryCompiler.cs` (`MimironDb2QueryExecutor`) | Accept `IDb2ModelBinding` instead of `IMimironDb2Db2ModelProvider`; call `GetBinding()` instead of `GetDb2Model()` |
| `QuerySession.cs` | Accept `Db2ModelBinding` instead of `Db2Model`; method signatures unchanged |
| `Db2QueryProvider.cs` | Accept `Db2ModelBinding` instead of `Db2Model`; method signatures unchanged |
| `Db2IncludeChainExecutor.cs` | Replace `Db2Model model` parameter → `Db2ModelBinding binding`; same call sites (method names are the same) |
| `Db2NavigationQueryCompiler.cs` | Replace `Db2Model model` parameter → `Db2ModelBinding binding` |
| `Db2NavigationQueryTranslator.cs` | Replace `Db2Model model` parameter → `Db2ModelBinding binding` |
| `Db2IncludePolicy.cs` | Replace `Db2Model model` parameter → `Db2ModelBinding binding` |
| `IMimironDb2Store.cs` | Change `TryMaterializeById` / `MaterializeByIds` signatures: replace `Db2Model` → `Db2ModelBinding` |
| `MimironDb2Store.cs` | Same as above (implementation) |

> **Note**: Because `Db2ModelBinding` exposes the exact same methods as `Db2Model` (`GetEntityType`, `TryGetReferenceNavigation`, `TryGetCollectionNavigation`, `GetAutoIncludeNavigations`), most consumer changes are a simple type rename with no logic changes.

### Phase 4 — Delete obsolete files

#### Source files to delete

| File | Reason |
|---|---|
| `src/.../Db2/Model/Db2Model.cs` | Replaced by `Db2ModelBinding` |
| `src/.../Db2/Model/Db2ModelBuilder.cs` | Replaced by `Db2ModelBinding` |
| `src/.../Db2/Model/Db2EntityTypeBuilder.cs` | Builder-only; no longer needed |
| `src/.../Db2/Model/Db2EntityTypeBuilder.Keys.cs` | Builder-only |
| `src/.../Db2/Model/Db2EntityTypeBuilder.Navigations.cs` | Builder-only |
| `src/.../Db2/Model/Db2EntityTypeBuilder.CollectionNavigations.cs` | Builder-only |
| `src/.../Db2/Model/Db2ReferenceNavigationBuilder.cs` | Builder-only |
| `src/.../Db2/Model/Db2CollectionNavigationBuilder.cs` | Builder-only |
| `src/.../Db2/Model/Db2PropertyBuilder.cs` | Builder-only |
| `src/.../Db2/Model/Db2EntityTypeMetadata.cs` | Builder-only metadata |
| `src/.../Db2/Model/Db2NavigationMetadata.cs` | Builder-only metadata |
| `src/.../Db2/Model/Db2CollectionNavigationMetadata.cs` | Builder-only metadata |
| `src/.../Query/MimironDb2Db2ModelProvider.cs` | Translation layer; replaced by `Db2ModelBindingProvider` |

#### Source files to evaluate for deletion

| File | Reason |
|---|---|
| `src/MimironSQL.Contracts/EntityFrameworkCore/Db2ModelBuildMode.cs` | Eager/Lazy enum; new binding is always lazy. Delete unless a public API contract depends on it elsewhere. |

#### Runtime types to **keep**

| File | Reason |
|---|---|
| `Db2EntityType.cs` | Runtime DTO consumed by materializer + query pipeline |
| `Db2ReferenceNavigation.cs` | Runtime DTO consumed by include executor + query compiler |
| `Db2CollectionNavigation.cs` | Runtime DTO consumed by include executor + query compiler |
| `Db2ReferenceNavigationKind.cs` | Enum used by above |
| `Db2CollectionNavigationKind.cs` | Enum used by above |

### Phase 5 — Update or rewrite tests

#### Test files that directly test the builder API (rewrite)

These tests validate builder behavior (HasOne, HasMany, WithForeignKey, etc.) and will be rewritten to test `Db2ModelBinding` directly by constructing a mock/test `IModel`.

| Test file | Action |
|---|---|
| `tests/.../Db2/Model/Db2ModelBuilderTests.cs` | Rewrite → `Db2ModelBindingTests.cs` |
| `tests/.../Db2/Model/Db2FluentBuilderTests.cs` | Rewrite → merge into `Db2ModelBindingTests.cs` |
| `tests/.../Db2/Model/Db2NavigationBuilderGuardTests.cs` | Rewrite → guard/validation tests for `Db2ModelBinding` |
| `tests/.../Db2/Model/SharedPrimaryKeyConfigurationTests.cs` | Rewrite → shared-PK scenario tests for `Db2ModelBinding` |

#### Test files that use `Db2ModelBuilder` in helpers (update)

These tests construct a `Db2ModelBuilder` in helper/setup code to produce a `Db2Model` for the component under test. They should be updated to construct a `Db2ModelBinding` (by building a mock `IModel` or using a shared test helper).

| Test file | Action |
|---|---|
| `tests/.../Db2/Query/Db2NavigationQueryCompilerTests.cs` | Update helper to build `Db2ModelBinding` |
| `tests/.../Db2/Query/Db2QueryProviderTests.cs` | Update helper |
| `tests/.../Db2/Query/Db2RowPredicateCompilerTests.cs` | Update helper |
| `tests/.../Db2/Query/Db2RowProjectorCompilerTests.cs` | Update helper |
| `tests/.../Db2/Query/Db2IncludePolicyTests.cs` | Update helper |
| `tests/.../Db2/Query/Db2IncludeChainExecutorTests.cs` | Update helper |
| `tests/.../Db2/Query/Db2IncludeChainExecutorForeignKeyArrayTests.cs` | Update helper |
| `tests/.../Db2/Query/Db2EntityMaterializerTests.cs` | Update helper |
| `tests/.../Db2/Query/Db2NavigationQueryTranslatorTests.cs` | Update helper |
| `tests/.../Db2/Query/Db2QueryProviderIQueryProviderSurfaceTests.cs` | Update helper |
| `tests/.../Query/QuerySessionTests.cs` | Update helper |

#### Integration tests (no changes expected)

| Test file | Action |
|---|---|
| `tests/.../CascDb2ContextIntegrationLocalTests.cs` | No code changes — these tests use `WoWDb2Context` and EF Core APIs only. They serve as end-to-end acceptance tests. |
| Other integration tests | Same — no direct Db2Model/Builder usage. |

### Phase 6 — Clean up contracts & options

1. **Delete `Db2ModelBuildMode`** from `MimironSQL.Contracts`.
2. **Remove `Db2ModelBuildMode` property** from `MimironDb2OptionsExtension` (and any builder method that sets it, e.g., `UseEagerDb2Model()` / `UseLazyDb2Model()` if they exist).
3. Remove `Db2ModelBuildMode` from option-comparison/hashing in `MimironDb2ModelCacheKeyFactory` if referenced.
4. Search for any remaining `using MimironSQL.EntityFrameworkCore.Db2.Model;` imports that reference deleted types and clean them up.

---

## Risk Assessment

| Risk | Mitigation |
|---|---|
| `Db2ModelBinding` introduces subtle behavioral differences (e.g., different column name resolution order) | Port validation logic line-by-line; run full test suite and integration tests after each phase |
| FK-array detection relies on EF `IForeignKey` which may not represent array-FK patterns | The FK-array pattern is configured today via `WithForeignKeyArray` builder call, which reads a `[ForeignKey]` attribute on an `int[]` property. `Db2ModelBinding` must detect this from the EF model's property metadata or annotations. Add a specific unit test for this scenario. |
| Lazy schema resolution semantics change | Today `BuildLazy` wraps each entity/navigation in `Lazy<T>`. `Db2ModelBinding` should do the same (`ConcurrentDictionary` + `Lazy<T>`). Behavior should be identical. |
| Test helper refactoring is large (14 test files) | Create a shared `TestDb2ModelBindingBuilder` helper that constructs a mock `IModel` + `Db2ModelBinding` for test scenarios. Reuse across all test files. |
| `MimironDb2Store.TryMaterializeById` / `MaterializeByIds` pass `Db2Model` — changing the signature is a cross-cutting change | These are `internal` APIs. The parameter type rename is safe. |

---

## Acceptance Criteria

### Build & Compilation

- [x] Solution compiles with zero errors (`dotnet build MimironSQL.slnx`).
- [x] No references to `Db2Model`, `Db2ModelBuilder`, `Db2EntityTypeBuilder`, `Db2ReferenceNavigationBuilder`, `Db2CollectionNavigationBuilder`, `Db2PropertyBuilder`, `Db2EntityTypeMetadata`, `Db2NavigationMetadata`, `Db2CollectionNavigationMetadata`, or `IMimironDb2Db2ModelProvider` remain in any source file.
- [x] No references to `Db2ModelBuildMode` remain in source or contracts.

### Deleted Files

- [x] All files listed in **Phase 4 — Delete obsolete files** are removed from the repository.
- [x] The following files are created:
  - `src/MimironSQL.EntityFrameworkCore/Db2/Model/Db2ModelBinding.cs`
  - `src/MimironSQL.EntityFrameworkCore/Db2/Model/IDb2ModelBinding.cs`
  - `src/MimironSQL.EntityFrameworkCore/Db2/Model/Db2ModelBindingProvider.cs`

### Retained Files

- [x] `Db2EntityType.cs`, `Db2ReferenceNavigation.cs`, `Db2CollectionNavigation.cs`, `Db2ReferenceNavigationKind.cs`, `Db2CollectionNavigationKind.cs` are undeleted and still compile.

### API Surface

- [x] `Db2ModelBinding` exposes: `GetEntityType(Type)`, `TryGetReferenceNavigation(Type, MemberInfo, out ...)`, `TryGetCollectionNavigation(Type, MemberInfo, out ...)`, `GetAutoIncludeNavigations(Type)`.
- [x] `IDb2ModelBinding` exposes: `GetBinding()`.
- [x] `Db2ModelBindingProvider` is registered as `IDb2ModelBinding` (scoped) in `MimironDb2ServiceCollectionExtensions`.

### Functional Behavior

- [x] All existing unit tests pass (after being updated to use `Db2ModelBinding`).
- [x] All integration tests pass without code changes (they do not reference the internal model layer).
- [x] `Db2ModelBinding` correctly resolves:
  - Entity types with table name from `[Table]` attribute and EF `GetTableName()`.
  - Primary key member from EF `FindPrimaryKey()`.
  - Column name mappings from EF `GetColumnName()`.
  - `SharedPrimaryKeyOneToOne` reference navigations (EF: `fk.IsUnique && dependent PK == FK`).
  - `ForeignKeyToPrimaryKey` reference navigations (EF: FK on declaring entity).
  - `DependentForeignKeyToPrimaryKey` collection navigations (EF: standard HasMany).
  - `ForeignKeyArrayToPrimaryKey` collection navigations (detects `int[]`/`ICollection<int>` FK-array properties).
  - Auto-include navigations (`INavigation.IsEagerLoaded`).
- [x] Schema resolution is lazy (first access triggers `.dbd` lookup).

### Tests

- [x] `Db2ModelBinding` tests cover: entity type registration, PK detection, column mapping, reference navigation kinds, collection navigation kinds, auto-include, FK-array detection, validation errors (missing PK, non-public getter, etc.).
- [x] Existing query pipeline tests (`Db2IncludeChainExecutorTests`, `Db2NavigationQueryCompilerTests`, etc.) pass after helper update.
- [x] A shared test helper exists for building `Db2ModelBinding` from a mock `IModel`.

### Performance

- [ ] No new allocations on the hot query path (schema + navigation lookups are cached after first access).
- [ ] No reflection on repeated calls (all reflection-based work happens once during lazy initialization).

### Code Quality

- [x] No `[MethodImpl]` attributes added.
- [x] No new public types introduced (all new types are `internal`).
- [x] No new NuGet packages required.

---

## Implementation Order

```
Phase 0   Preparation / read options extension
  ↓
Phase 1   Create Db2ModelBinding.cs
  ↓
Phase 2   Create IDb2ModelBinding + Db2ModelBindingProvider
  ↓
Phase 3   Update all source consumers (type rename Db2Model → Db2ModelBinding)
  ↓
Phase 4   Delete obsolete files
  ↓
Phase 5   Update / rewrite tests
  ↓
Phase 6   Clean up contracts & options
  ↓
  ✓       dotnet build + dotnet test → all green
```