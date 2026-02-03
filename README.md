# MimironSQL

MimironSQL is a high-performance LINQ query provider for World of Warcraft's DB2 database files. It enables type-safe querying of game data using familiar LINQ syntax, with support for extracted `.db2` files from the filesystem.

## Features

- **Type-safe LINQ queries** - Query DB2 files using standard LINQ operations
- **Navigation properties** - Configure and traverse relationships between tables
- **Multiple data sources** - Read from the filesystem
- **High performance** - Optimized for efficient data access and materialization
- **Read-only** - Safe, non-destructive access to game data

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

Create classes that represent your DB2 tables by inheriting from `Db2Entity`:

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
- `Db2Entity` - Base class for entities with `int` primary key
- `Db2LongEntity` - For entities with `long` primary key
- `Db2GuidEntity` - For entities with `Guid` primary key
- `Db2StringEntity` - For entities with `string` primary key

### 2. Create a DB2 Context

Define a context class that inherits from `Db2Context` and exposes your tables:

```csharp
using MimironSQL.Db2.Query;
using MimironSQL.Db2.Model;
using MimironSQL.Providers;

public class WowDb2Context : Db2Context
{
    public Db2Table<Map> Map { get; init; } = null!;
    public Db2Table<MapChallengeMode> MapChallengeMode { get; init; } = null!;
    public Db2Table<Spell> Spell { get; init; } = null!;
    
    public WowDb2Context(IDbdProvider dbdProvider, IDb2StreamProvider db2StreamProvider)
        : base(dbdProvider, db2StreamProvider)
    {
    }
    
    protected override void OnModelCreating(Db2ModelBuilder modelBuilder)
    {
        // Configure navigations (optional)
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

### 3. Configure Providers

Set up providers for DB2 files and schema definitions:

```csharp
using MimironSQL.Providers;

// Configure filesystem paths
var dbdProvider = new FileSystemDbdProvider(
    new FileSystemDbdProviderOptions(@"C:\WoWDBDefs\definitions"));

var db2StreamProvider = new FileSystemDb2StreamProvider(
    new FileSystemDb2StreamProviderOptions(@"C:\WoW\DBFilesClient"));

// Create the context
var context = new WowDb2Context(dbdProvider, db2StreamProvider);
```

### 4. Query Your Data

Use standard LINQ to query DB2 files:

```csharp
// Simple queries
var maps = context.Map
    .Where(m => m.Directory.Contains("raid"))
    .ToList();

// Projections
var mapNames = context.Map
    .Select(m => new { m.Id, m.MapName_lang })
    .ToList();

// Navigation properties
var challengeModeWithMap = context.MapChallengeMode
    .Include(mc => mc.Map)
    .Where(mc => mc.MapID == 2441)
    .ToList();

// Direct key lookup (fastest)
var specificMap = context.Map.Find(1);
```

## Configuration

### Providers

MimironSQL uses two provider interfaces:

#### IDb2StreamProvider

Provides access to DB2 file streams.

**FileSystemDb2StreamProvider** - Reads DB2 files from disk:

```csharp
var db2Provider = new FileSystemDb2StreamProvider(
    new FileSystemDb2StreamProviderOptions(
        Db2DirectoryPath: @"C:\WoW\DBFilesClient"
    ));
```

#### IDbdProvider

Provides access to DBD (Database Definition) files from WoWDBDefs.

**FileSystemDbdProvider** - Reads DBD files from disk:

```csharp
var dbdProvider = new FileSystemDbdProvider(
    new FileSystemDbdProviderOptions(
        DefinitionsDirectory: @"C:\WoWDBDefs\definitions"
    ));
```

### Format Registration

MimironSQL automatically registers WDC5 format support. To use a different format:

```csharp
using MimironSQL.Formats.Wdc5;

public class WowDb2Context : Db2Context
{
    public WowDb2Context(IDbdProvider dbdProvider, IDb2StreamProvider db2StreamProvider)
        : base(dbdProvider, db2StreamProvider)
    {
        // WDC5 is registered by default
        // Or explicitly register:
        RegisterFormat(Wdc5Format.Register);
    }
}
```

## Public API Reference

### Core Classes

#### `Db2Context`

Abstract base class for creating DB2 contexts.

**Constructor:**
```csharp
protected Db2Context(IDbdProvider dbdProvider, IDb2StreamProvider db2StreamProvider)
```

**Methods:**
- `void RegisterFormat(params IDb2Format[] formats)` - Register DB2 format handlers
- `void RegisterFormat(Action<Db2FormatRegistry> registerFormats)` - Register formats using a callback
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

MimironSQL supports the following LINQ query operations:

- `Where` - Filtering
- `Select` - Projection (including anonymous types and navigation properties)
- `FirstOrDefault`, `First` - Retrieve first element
- `SingleOrDefault`, `Single` - Retrieve single element
- `Take`, `Skip` - Result limiting and pagination
- `Count`, `Any` - Aggregation
- `Include` - Eager loading of navigation properties
- `Find` - Direct primary key lookup (most efficient)

**Note:** Unsupported LINQ operations will throw `NotSupportedException` at query execution time. Write operations (Insert, Update, Delete) are not supported as DB2 files are read-only.

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

### "No .db2 file found for table"
Ensure your DB2 files directory contains the required `.db2` files and the table name matches the file name (case-insensitive).

### "Field not found in schema"
Your entity property names must match the field names in the corresponding `.dbd` definition file from WoWDBDefs. Check the definition for exact field names.

### "Entity type has no key member"
Ensure your entity class:
1. Inherits from `Db2Entity` (or variant), OR
2. Has a property named `Id`, OR
3. Has a key configured via `modelBuilder.Entity<T>().HasKey(e => e.CustomId)`

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