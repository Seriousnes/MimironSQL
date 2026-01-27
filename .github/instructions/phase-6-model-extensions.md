# Phase 6 Model Extensions — Design Notes

**Status:** Design only (not implemented until Phases 3–5 are complete)

This document outlines the model extensions needed to support collection navigations and multi-hop navigation metadata.

---

## Collection Navigation Metadata

### `Db2CollectionNavigation`

Represents a one-to-many navigation from a principal entity to a collection of dependent entities.

```csharp
namespace MimironSQL.Db2.Model;

/// <summary>
/// Metadata for a collection navigation (1-to-many relationship).
/// </summary>
/// <param name="principalClrType">The principal (parent) entity CLR type</param>
/// <param name="collectionMember">The collection property on the principal entity (e.g., ICollection&lt;Child&gt;)</param>
/// <param name="dependentClrType">The dependent (child) entity CLR type</param>
/// <param name="foreignKeyMember">The foreign key member on the dependent entity</param>
/// <param name="principalKeyMember">The principal key member on the principal entity</param>
/// <param name="foreignKeyFieldSchema">Resolved field schema for the foreign key</param>
/// <param name="principalKeyFieldSchema">Resolved field schema for the principal key</param>
/// <param name="inverseNavigation">Optional inverse reference navigation from dependent to principal</param>
public sealed class Db2CollectionNavigation(
    Type principalClrType,
    MemberInfo collectionMember,
    Type dependentClrType,
    MemberInfo foreignKeyMember,
    MemberInfo principalKeyMember,
    Db2FieldSchema foreignKeyFieldSchema,
    Db2FieldSchema principalKeyFieldSchema,
    MemberInfo? inverseNavigation)
{
    public Type PrincipalClrType { get; } = principalClrType;
    public MemberInfo CollectionMember { get; } = collectionMember;
    public Type DependentClrType { get; } = dependentClrType;
    
    /// <summary>
    /// Foreign key member on the dependent entity (e.g., SpellEffect.SpellID)
    /// </summary>
    public MemberInfo ForeignKeyMember { get; } = foreignKeyMember;
    
    /// <summary>
    /// Principal key member on the principal entity (e.g., Spell.Id)
    /// </summary>
    public MemberInfo PrincipalKeyMember { get; } = principalKeyMember;
    
    public Db2FieldSchema ForeignKeyFieldSchema { get; } = foreignKeyFieldSchema;
    public Db2FieldSchema PrincipalKeyFieldSchema { get; } = principalKeyFieldSchema;
    
    /// <summary>
    /// Optional inverse navigation from dependent back to principal (e.g., SpellEffect.Spell)
    /// </summary>
    public MemberInfo? InverseNavigation { get; } = inverseNavigation;
}
```

### Storage in `Db2Model`

`Db2Model` would need to store collection navigations separately from reference navigations:

```csharp
public sealed class Db2Model
{
    // Existing:
    private readonly Dictionary<(Type entityType, string memberName), Db2ReferenceNavigation> _referenceNavigations;
    
    // New for Phase 6:
    private readonly Dictionary<(Type entityType, string memberName), Db2CollectionNavigation> _collectionNavigations;
    
    public bool TryGetCollectionNavigation(
        Type entityType,
        MemberInfo member,
        out Db2CollectionNavigation navigation)
    {
        // Lookup implementation
    }
}
```

---

## Configuration API Extensions

### `Db2EntityTypeBuilder` Extensions

Add `HasMany(...)` and `HasOne(...)` fluent API for configuring collection navigations:

```csharp
public sealed class Db2EntityTypeBuilder<TEntity>
{
    // Existing methods: HasKey, HasOne (reference navigations)
    
    /// <summary>
    /// Configures a one-to-many relationship.
    /// </summary>
    public Db2CollectionNavigationBuilder<TEntity, TRelated> HasMany<TRelated>(
        Expression<Func<TEntity, ICollection<TRelated>>> navigationExpression)
    {
        // Extract collection member from expression
        // Return builder for chaining .WithOne(...).HasForeignKey(...)
    }
}

public sealed class Db2CollectionNavigationBuilder<TPrincipal, TDependent>
{
    /// <summary>
    /// Configures the inverse navigation from dependent to principal.
    /// </summary>
    public Db2CollectionNavigationBuilder<TPrincipal, TDependent> WithOne(
        Expression<Func<TDependent, TPrincipal?>>? inverseNavigationExpression = null)
    {
        // Configure inverse navigation (optional)
        return this;
    }
    
    /// <summary>
    /// Configures the foreign key on the dependent entity.
    /// </summary>
    public Db2CollectionNavigationBuilder<TPrincipal, TDependent> HasForeignKey<TKey>(
        Expression<Func<TDependent, TKey>> foreignKeyExpression)
    {
        // Extract foreign key member from expression
        // Validate against schema
        return this;
    }
    
    /// <summary>
    /// Configures the principal key on the principal entity.
    /// Defaults to the primary key if not specified.
    /// </summary>
    public Db2CollectionNavigationBuilder<TPrincipal, TDependent> HasPrincipalKey<TKey>(
        Expression<Func<TPrincipal, TKey>> principalKeyExpression)
    {
        // Extract principal key member from expression
        return this;
    }
}
```

### Example Usage

```csharp
protected override void OnModelCreating(Db2ModelBuilder modelBuilder)
{
    modelBuilder.Entity<Spell>()
        .HasMany(s => s.SpellEffects)
        .WithOne(e => e.Spell)
        .HasForeignKey(e => e.SpellID);
    
    // Compound relationships
    modelBuilder.Entity<Spell>()
        .HasMany(s => s.SpellVisuals)
        .WithOne()  // no inverse navigation
        .HasForeignKey(v => v.SpellID);
}
```

---

## Multi-Hop Navigation Metadata

Multi-hop navigations don't require new navigation types — they are composed from existing reference and collection navigations.

### Navigation Chain Representation

During query translation, multi-hop navigation access is resolved into a **navigation chain**:

```csharp
/// <summary>
/// Represents a resolved navigation chain for multi-hop navigation access.
/// </summary>
public sealed class Db2NavigationChain
{
    public Type RootClrType { get; }
    public Type LeafClrType { get; }
    
    /// <summary>
    /// Ordered list of navigation hops from root to leaf.
    /// Each hop is either a reference navigation or a collection navigation.
    /// </summary>
    public IReadOnlyList<Db2NavigationHop> Hops { get; }
}

public sealed class Db2NavigationHop
{
    public Type SourceClrType { get; }
    public MemberInfo NavigationMember { get; }
    public Type TargetClrType { get; }
    
    /// <summary>
    /// The navigation metadata (either Db2ReferenceNavigation or Db2CollectionNavigation)
    /// </summary>
    public object NavigationMetadata { get; }
    
    public bool IsCollectionNavigation => NavigationMetadata is Db2CollectionNavigation;
}
```

### Example: 2-hop Chain

For `Spell.SpellName.Name_lang`:

```
Hop 1: Spell -> SpellName (reference navigation, FK-based)
Hop 2: SpellName.Name_lang (member access on target entity)
```

Navigation chain:
- Root: `Spell`
- Hop 1: Reference navigation `Spell.SpellName` → `SpellName`
- Leaf: `SpellName.Name_lang` (string field)

### Example: Mixed Chain with Collection

For `Spell.SpellEffects.Any(e => e.Effect == 53)`:

```
Hop 1: Spell -> SpellEffects (collection navigation)
Hop 2: Predicate on SpellEffect.Effect field
```

Navigation chain:
- Root: `Spell`
- Hop 1: Collection navigation `Spell.SpellEffects` → `ICollection<SpellEffect>`
- Leaf: Predicate on `SpellEffect` entities

---

## Query Plan Extensions

### Multi-Source Plans with Collection Navigations

Collection navigations require **one-to-many join semantics** in the query plan:

```csharp
public sealed class Db2CollectionNavigationJoinPlan
{
    public Db2EntityType Principal { get; }
    public Db2CollectionNavigation Navigation { get; }
    public Db2EntityType Dependent { get; }
    
    /// <summary>
    /// Member on principal entity for the join key
    /// </summary>
    public MemberInfo PrincipalKeyMember { get; }
    
    /// <summary>
    /// Member on dependent entity for the foreign key
    /// </summary>
    public MemberInfo DependentForeignKeyMember { get; }
}
```

### Multi-Hop Query Plans

Multi-hop navigation plans include intermediate sources:

```csharp
public sealed class Db2MultiHopNavigationQueryPlan
{
    public Db2EntityType Root { get; }
    public IReadOnlyList<Db2NavigationJoinPlan> JoinChain { get; }
    public Db2EntityType Leaf { get; }
    
    /// <summary>
    /// Required columns per source in the chain (root, intermediate tables, leaf)
    /// </summary>
    public IReadOnlyDictionary<Db2EntityType, Db2SourceRequirements> RequirementsPerSource { get; }
}
```

---

## Execution Strategy Extensions

### Collection Navigation Loading

#### For `Include(...)`

```csharp
internal sealed class Db2BatchedCollectionLoader
{
    public Dictionary<object, List<object>> LoadCollectionBatch(
        Db2CollectionNavigationJoinPlan join,
        IEnumerable<object> principalKeys,
        Db2SourceRequirements dependentRequirements)
    {
        // 1. Collect distinct principal keys from the root result set
        // 2. Scan dependent table WHERE dependent_fk IN (principal_keys)
        // 3. Group dependents by foreign key value
        // 4. Return lookup: principal_key -> List<dependent>
    }
}
```

#### For Collection Predicates (`Any`, `All`, `Count`)

**`Any(predicate)` — Semi-Join:**

```csharp
// 1. Evaluate predicate on dependent table
var matchingForeignKeys = dependentTable
    .Where(dependent => predicate(dependent))
    .Select(dependent => dependent.ForeignKey)
    .ToHashSet();

// 2. Filter principal table by key membership
var results = principalTable
    .Where(principal => matchingForeignKeys.Contains(principal.Key));
```

**`Count()` — Aggregation:**

```csharp
// Group dependents by foreign key and count
var countsPerPrincipal = dependentTable
    .GroupBy(dependent => dependent.ForeignKey)
    .ToDictionary(g => g.Key, g => g.Count());

// Project principal with count
var results = principalTable
    .Select(principal => new
    {
        principal,
        Count = countsPerPrincipal.TryGetValue(principal.Key, out var c) ? c : 0
    });
```

### Multi-Hop Navigation Loading

#### Semi-Join Propagation (for predicates)

For `Root.Nav1.Nav2.Field == value`:

```
1. Scan Nav2 table WHERE Nav2.Field == value → collect Nav2_keys
2. Scan Nav1 table WHERE Nav1.Nav2_FK IN (Nav2_keys) → collect Nav1_keys
3. Scan Root table WHERE Root.Nav1_FK IN (Nav1_keys) → final results
```

#### Batched Lookup Chain (for projections)

For `Select(x => x.Nav1.Nav2.Field)`:

```
1. Collect distinct Root.Nav1_FK values from root scan
2. Batch load Nav1 rows: lookup1 = { Nav1_PK -> Nav1_row }
3. Collect distinct Nav1.Nav2_FK values from lookup1
4. Batch load Nav2 rows: lookup2 = { Nav2_PK -> Nav2_row }
5. For each root row:
   - Lookup Nav1 via Root.Nav1_FK
   - Lookup Nav2 via Nav1.Nav2_FK
   - Project Nav2.Field
```

---

## Testing Considerations

### Fixture Data Requirements

Phase 6 tests must use existing fixture data. Example relationships in fixture DB2s:

- `Spell` → `SpellEffect` (1-to-many via `SpellEffect.SpellID`)
- `Spell` → `SpellName` (1-to-1 via shared primary key or FK)
- `Spell` → `SpellAuraOptions` → `SpellAuraEffect` (2-hop, mixed)

### Test Cases

**Collection navigations:**
- `Include(s => s.SpellEffects)` loads all effects in one batch
- `Where(s => s.SpellEffects.Any(e => e.Effect == X))` uses semi-join
- `Select(s => s.SpellEffects.Count())` uses aggregation
- Principal with no dependents returns empty collection (not null)

**Multi-hop navigations:**
- `Where(s => s.SpellName.Name_lang.Contains("Fire"))` uses 2-hop semi-join
- `Select(s => s.SpellName.Name_lang)` uses 2-hop batched lookup
- `Where(s => s.SpellName == null)` handles missing intermediate navigations correctly
- 3-hop chains are tested for correctness and performance

**Performance:**
- Prove no N+1 queries using `Wdc5FileLookupTracker` or similar
- Prove reduced entity materialization using constructor counters
- Validate batch sizes are reasonable (not loading entire tables)

---

## Open Implementation Questions

1. **Collection cardinality limits:** Max collection size per principal? Configurable? Fail fast or lazy load on overflow?
2. **Collection navigation materialization:** Always materialize full entities, or support column pruning on dependents?
3. **Inverse navigation population:** Auto-populate inverse reference navigations (e.g., `SpellEffect.Spell`) when loading via `Include`?
4. **Multi-hop depth limit:** Hard limit (e.g., 3 hops max) or configurable?
5. **Null propagation for multi-hop predicates:** Strict (filter out nulls) or null-safe (propagate null as false)?
6. **Collection predicate closure support:** Can `Any(e => e.Field == capturedVariable)` work, or only constants?

---

## Related Files

- Phase 6 design document: `./phase-6-design.md`
- Implementation plan: `./implementation-plan.md`
- Existing reference navigation: `MimironSQL/Db2/Model/Db2ReferenceNavigation.cs`
- Existing navigation builder: `MimironSQL/Db2/Model/Db2ReferenceNavigationBuilder.cs`
- Existing entity type builder: `MimironSQL/Db2/Model/Db2EntityTypeBuilder.cs`

---

**Note:** This is a design document only. Do not implement these types until Phases 3–5 are complete and stable.
