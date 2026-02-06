# MimironSQL

MimironSQL is a high-performance, **Entity Framework Core-like** query provider for World of Warcraft's DB2 database files. It provides a familiar `DbContext`-based API for querying game data using LINQ, with full support for navigation properties and relationships.

## Features

- **EF Core-like API** - Familiar `DbContext`, entities, and LINQ patterns
- **Type-safe LINQ queries** - Query DB2 files using standard LINQ operations
- **Navigation properties** - Configure and traverse relationships between tables  
- **Multiple data sources** - Read from filesystem or CASC archives
- **High performance** - Optimized for efficient data access and materialization
- **Read-only by design** - Safe, non-destructive access to game data

## Important Limitations

⚠️ **MimironSQL is read-only**. The following operations are **NOT supported**:
- `SaveChanges()` - DB2 files cannot be modified
- Database migrations - Schema is defined by WoWDBDefs
- Raw SQL queries - Only LINQ is supported
- Change tracking - All queries use no-tracking by default

## Installation

MimironSQL is a .NET library targeting .NET 10. To use it in your project, add a reference to the MimironSQL projects:

```xml
<ItemGroup>
  <ProjectReference Include="..\MimironSQL\MimironSQL.csproj" />
  <ProjectReference Include="..\MimironSQL.Formats.Wdc5\MimironSQL.Formats.Wdc5.csproj" />
</ItemGroup>
```

### Dependencies

MimironSQL requires:
- **.NET 10.0 or later**
- **WoWDBDefs definitions** - Database schema definitions from [WoWDBDefs](https://github.com/wowdev/WoWDBDefs)

## Quick Start

### 1. Define Your Entities

Create entity classes that represent your DB2 tables. Inherit from `Db2Entity` (or variant) to define the primary key type:

```csharp
using MimironSQL.Db2;

public class Map : Db2Entity
{
    public required string Directory { get; set; }
    public required string MapName_lang { get; set; }
    public int ParentMapID { get; set; }
    
    // Navigation properties
    public Map? ParentMap { get; set; }
    public ICollection<MapChallengeMode> MapChallengeModes { get; set; } = null!;
}

public class MapChallengeMode : Db2Entity
{
    public required ushort MapID { get; set; }
    public required string Name_lang { get; set; }
    
    // Navigation property
    public Map? Map { get; set; }
}
```

**Entity Base Classes:**
- `Db2Entity` - Entities with `int` primary key (most common)
- `Db2LongEntity` - Entities with `long` primary key  
- `Db2GuidEntity` - Entities with `Guid` primary key
- `Db2StringEntity` - Entities with `string` primary key

### 2. Create a DbContext

Define a context class that inherits from `Db2Context` and exposes `Db2Table<T>` properties for your entities:

```csharp
using MimironSQL.Db2.Query;
using MimironSQL.Db2.Model;
using MimironSQL.Formats.Wdc5;
using MimironSQL.Providers;

public class WowDb2Context : Db2Context
{
    public Db2Table<Map> Map { get; init; } = null!;
    public Db2Table<MapChallengeMode> MapChallengeMode { get; init; } = null!;
    public Db2Table<Spell> Spell { get; init; } = null!;
    
    public WowDb2Context(IDbdProvider dbdProvider, IDb2StreamProvider db2StreamProvider, IDb2Format format)
        : base(dbdProvider, db2StreamProvider, format)
    {
    }
    
    protected override void OnModelCreating(Db2ModelBuilder modelBuilder)
    {
        // Configure relationships (optional - many are auto-detected from schema)
        modelBuilder
            .Entity<MapChallengeMode>()
            .HasOne(mc => mc.Map)
            .WithForeignKey(mc => mc.MapID);
            
        modelBuilder
            .Entity<Map>()
            .HasMany(m => m.MapChallengeModes)
            .WithForeignKey(mc => mc.MapID);
    }
}
```

### 3. Configure Providers and Create Context

#### Option A: FileSystem Provider

For reading `.db2` files extracted from the game client:

```csharp
using MimironSQL.Providers;
using MimironSQL.Formats.Wdc5;

// Configure filesystem paths
var dbdProvider = new FileSystemDbdProvider(
    new FileSystemDbdProviderOptions(@"C:\WoWDBDefs\definitions"));

var db2StreamProvider = new FileSystemDb2StreamProvider(
    new FileSystemDb2StreamProviderOptions(@"C:\WoW\DBFilesClient"));

// Create the context with WDC5 format support
var context = new WowDb2Context(dbdProvider, db2StreamProvider, Wdc5Format.Instance);
context.EnsureModelCreated();
```

#### Option B: CASC Provider

For reading DB2 files directly from CASC archives (no extraction needed):

```csharp
using MimironSQL.Providers;
using MimironSQL.Formats.Wdc5;
using Microsoft.Extensions.DependencyInjection;

// Register CASC services with dependency injection
var services = new ServiceCollection();
services.AddCascNet(
    configureCascNet: options =>
    {
        options.WowInstallRoot = @"C:\Program Files (x86)\World of Warcraft";
    });

var serviceProvider = services.BuildServiceProvider();

// Get the CASC DB2 provider
var db2StreamProvider = serviceProvider.GetRequiredService<IDb2StreamProvider>();
var dbdProvider = new FileSystemDbdProvider(
    new FileSystemDbdProviderOptions(@"C:\WoWDBDefs\definitions"));

// Create the context
var context = new WowDb2Context(dbdProvider, db2StreamProvider, Wdc5Format.Instance);
context.EnsureModelCreated();
```

### 4. Query Your Data

Use standard LINQ operations to query DB2 files. All queries are **read-only** and **no-tracking** by default:

```csharp
// Simple filtering
var raidMaps = context.Map
    .Where(m => m.Directory.Contains("raid"))
    .ToList();

// Projections  
var mapNames = context.Map
    .Select(m => new { m.Id, m.MapName_lang })
    .ToList();

// Navigation properties with Include()
var modesWithMap = context.MapChallengeMode
    .Include(mc => mc.Map)
    .Where(mc => mc.MapID == 2441)
    .ToList();

// Direct primary key lookup (most efficient)
var specificMap = context.Map.Find(1);

// Count and aggregations
var mapCount = context.Map.Where(m => m.Id > 0).Count();
```

## Configuration

### Data Providers

MimironSQL uses a provider architecture for accessing DB2 files and their schemas.

#### IDb2StreamProvider

Provides access to DB2 file streams from various sources.

**FileSystemDb2StreamProvider** - Read extracted `.db2` files from disk:

```csharp
var db2Provider = new FileSystemDb2StreamProvider(
    new FileSystemDb2StreamProviderOptions(
        Db2DirectoryPath: @"C:\WoW\DBFilesClient"
    ));
```

**CascDBCProvider** - Read DB2 files directly from CASC archives:

```csharp
// Use with dependency injection - see AddCascNet() extension method
services.AddCascNet(
    configureCascNet: options =>
    {
        options.WowInstallRoot = @"C:\Program Files (x86)\World of Warcraft";
    });
```

#### IDbdProvider

Provides access to DBD (Database Definition) files from WoWDBDefs that describe the schema of each DB2 table.

**FileSystemDbdProvider** - Read `.dbd` definition files from disk:

```csharp
var dbdProvider = new FileSystemDbdProvider(
    new FileSystemDbdProviderOptions(
        DefinitionsDirectory: @"C:\WoWDBDefs\definitions"
    ));
```

### Format Support

MimironSQL requires a format handler to read the binary DB2 files. Currently supported:

- **WDC5** - World of Warcraft DB2 format version 5 (Legion and later)

```csharp
using MimironSQL.Formats.Wdc5;

public class WowDb2Context : Db2Context
{
    public WowDb2Context(IDbdProvider dbdProvider, IDb2StreamProvider db2StreamProvider)
        : base(dbdProvider, db2StreamProvider, Wdc5Format.Instance)
    {
    }
}
```

## Public API Reference

### Core Classes

#### `Db2Context`

Abstract base class for creating DB2 contexts.

**Constructor:**
```csharp
protected Db2Context(IDbdProvider dbdProvider, IDb2StreamProvider db2StreamProvider, IDb2Format format)
```

**Methods:**
- `void EnsureModelCreated()` - Initialize the context model (must be called before querying)
- `protected Db2Table<T> Table<T>(string? tableName = null)` - Access a table by entity type
- `protected virtual void OnModelCreating(Db2ModelBuilder modelBuilder)` - Configure entity relationships

#### `Db2Table<T>`

Represents a queryable DB2 table.

**Properties:**
- `Db2TableSchema Schema` - Schema information for the table
- `IQueryProvider Provider` - Query provider for LINQ execution

**Methods:**
- `T? Find<TId>(TId id)` - Find entity by primary key (fastest lookup method)

#### `Db2Entity<TId>` / `Db2Entity`

Base classes for DB2 entities.

**Properties:**
- `TId Id { get; set; }` - Primary key property

**Derived Classes:**
- `Db2Entity` - For `int` keys
- `Db2LongEntity` - For `long` keys
- `Db2GuidEntity` - For `Guid` keys
- `Db2StringEntity` - For `string` keys

### Model Configuration

#### `Db2ModelBuilder`

Fluent API for configuring entity relationships.

**Methods:**
- `Db2EntityTypeBuilder<T> Entity<T>()` - Configure an entity type
- `Db2ModelBuilder ApplyConfiguration<T>(IDb2EntityTypeConfiguration<T> configuration)` - Apply external configuration
- `Db2ModelBuilder ApplyConfigurationsFromAssembly(params Assembly[] assemblies)` - Scan and apply all configurations

#### `Db2EntityTypeBuilder<T>`

Configure a specific entity type.

**Methods:**
- `Db2EntityTypeBuilder<T> ToTable(string tableName)` - Map to a specific table name
- `Db2ReferenceNavigationBuilder<T, TTarget> HasOne<TTarget>(Expression<Func<T, TTarget?>> navigation)` - Configure a one-to-one/many-to-one relationship
- `Db2CollectionNavigationBuilder<T, TTarget> HasMany<TTarget>(Expression<Func<T, IEnumerable<TTarget>>> navigation)` - Configure a one-to-many relationship

#### `Db2ReferenceNavigationBuilder<TSource, TTarget>`

Configure reference (one-to-one/many-to-one) navigations.

**Methods:**
- `WithSharedPrimaryKey<TKey>(Expression<Func<TSource, TKey>> sourceKey, Expression<Func<TTarget, TKey>> targetKey)` - Configure shared primary key relationship
- `WithForeignKey<TKey>(Expression<Func<TSource, TKey>> foreignKey)` - Specify foreign key property
- `HasPrincipalKey<TKey>(Expression<Func<TTarget, TKey>> principalKey)` - Specify principal key (target)
- `OverridesSchema()` - Mark navigation as overriding schema conventions

#### `Db2CollectionNavigationBuilder<TSource, TTarget>`

Configure collection (one-to-many) navigations.

**Methods:**
- `WithForeignKey<TKey>(Expression<Func<TTarget, TKey>> foreignKey)` - Specify foreign key on dependent entity
- `WithForeignKeyArray(Expression<Func<TSource, IEnumerable<int>>> foreignKeyIds)` - Configure array-based foreign keys
- `HasPrincipalKey<TKey>(Expression<Func<TSource, TKey>> principalKey)` - Specify principal key
- `OverridesSchema()` - Mark navigation as overriding schema conventions

### Query Extensions

#### `Db2QueryableExtensions`

LINQ extensions for DB2 queries.

**Methods:**
- `IQueryable<TEntity> Include<TEntity, TProperty>(this IQueryable<TEntity> source, Expression<Func<TEntity, TProperty>> navigationPropertyPath)` - Eagerly load navigation properties

## Supported LINQ Operations

MimironSQL supports a rich set of LINQ query operations:

### Query Operations
- `Where()` - Filtering with predicates
- `Select()` - Projection (including anonymous types and navigation properties)
- `Include()` - Eager loading of navigation properties
- `Take()`, `Skip()` - Limiting and pagination
- `OrderBy()`, `ThenBy()` - Ordering (client-side evaluation)

### Terminal Operations
- `ToList()`, `ToArray()` - Materialize results
- `First()`, `FirstOrDefault()` - Retrieve first element
- `Single()`, `SingleOrDefault()` - Retrieve single element
- `Count()`, `Any()`, `All()` - Aggregation operations
- `Find()` - Direct primary key lookup (**most efficient**)

**Important Notes:**
- All queries are **read-only** and **no-tracking** by default
- Unsupported LINQ operations will throw `NotSupportedException` at execution time
- Write operations (`Add`, `Update`, `Remove`, `SaveChanges`) are **not supported**
- Raw SQL queries are **not supported** - use LINQ only

## Navigation Configuration Examples

### One-to-One (Shared Primary Key)

```csharp
modelBuilder
    .Entity<Spell>()
    .HasOne(s => s.SpellName)
    .WithSharedPrimaryKey(s => s.Id, sn => sn.Id);
```

### Many-to-One (Foreign Key)

```csharp
modelBuilder
    .Entity<MapChallengeMode>()
    .HasOne(mc => mc.Map)
    .WithForeignKey(mc => mc.MapID);
```

### One-to-Many (Inverse of Many-to-One)

```csharp
modelBuilder
    .Entity<Map>()
    .HasMany(m => m.MapChallengeModes)
    .WithForeignKey(mc => mc.MapID);
```

### One-to-Many (Array of Foreign Keys)

```csharp
modelBuilder
    .Entity<MapChallengeMode>()
    .HasMany(m => m.FirstRewardQuest)
    .WithForeignKeyArray(m => m.FirstRewardQuestID);
```

### Schema Override

If your model conflicts with schema conventions:

```csharp
modelBuilder
    .Entity<MyEntity>()
    .HasOne(e => e.CustomNavigation)
    .WithForeignKey(e => e.CustomFK)
    .OverridesSchema();
```

## Advanced Usage

### Custom Table Names

```csharp
modelBuilder
    .Entity<CustomEntity>()
    .ToTable("ActualTableName");
```

### External Configuration Classes

```csharp
public class MapConfiguration : IDb2EntityTypeConfiguration<Map>
{
    public void Configure(Db2EntityTypeBuilder<Map> builder)
    {
        builder.ToTable("Map");
        builder.HasMany(m => m.MapChallengeModes)
               .WithForeignKey(mc => mc.MapID);
    }
}

// In OnModelCreating:
modelBuilder.ApplyConfigurationsFromAssembly(Assembly.GetExecutingAssembly());
```

### Accessing Raw Files

For advanced scenarios, you can access the underlying DB2 format:

```csharp
// Access through context internals (use with caution)
var (file, schema) = context.GetOrOpenTableRaw("Map");
```

## Troubleshooting

### "The context model has not been created"
Call `context.EnsureModelCreated()` after creating your context and before running any queries.

### "No .db2 file found for table"
Ensure your DB2 files directory contains the required `.db2` files and the table name matches the file name (case-insensitive).

### "Field not found in schema"
Entity property names must match the field names in the corresponding `.dbd` definition file from WoWDBDefs. Check the definition for exact field names.

### "Entity type has no key member"
Ensure your entity class:
1. Inherits from `Db2Entity` (or variant), OR
2. Has a property named `Id`, OR
3. Has a key configured via `modelBuilder.Entity<T>().HasKey(e => e.CustomId)`

### "SaveChanges not supported" or "Migrations not supported"
This is **expected behavior**. MimironSQL is **read-only** by design:
- DB2 files cannot be modified
- No `SaveChanges()` support
- No migration support - schema is defined by WoWDBDefs
- No raw SQL support - use LINQ queries only

### Navigation Not Working
1. Check that the navigation is configured in `OnModelCreating`
2. Verify foreign key property names match the DBD schema
3. Use `OverridesSchema()` if your configuration conflicts with auto-detected schema conventions

## Performance Tips

1. **Use `Find(id)` for single-record lookups** - This is the fastest way to retrieve a record by primary key
2. **Limit results with `Take()`** - Avoid materializing entire tables
3. **Project only needed columns** - Use `Select()` to project only required fields
4. **Use `Include()` sparingly** - Only eager-load navigations when needed

## Requirements

- .NET 10.0 or later
- WoWDBDefs definition files (`.dbd` files)
- World of Warcraft DB2 files (`.db2` files)

## License

This project is licensed under the MIT License. See [LICENSE.txt](LICENSE.txt) for details.

For acknowledgments of specifications and community resources used in this project, see [THIRD_PARTY_NOTICES.md](THIRD_PARTY_NOTICES.md).

## Contributing

Contributions are welcome! Please ensure all tests pass before submitting pull requests.

```bash
# Build the project
dotnet build MimironSQL.slnx

# Run tests
dotnet test MimironSQL.slnx
```