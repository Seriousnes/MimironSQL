# MimironSQL.EntityFrameworkCore

Entity Framework Core database provider for World of Warcraft DB2 files. Enables querying DB2 files using standard EF Core LINQ queries with full support for relationships, projections, and eager loading.

## Overview

`MimironSQL.EntityFrameworkCore` is a read-only EF Core database provider that treats DB2 files as queryable data sources. It provides:

- Full LINQ query support (Where, Select, Include, Take, Skip, etc.)
- Relationship navigation and eager loading
- Lazy loading support via proxies
- Integration with WoWDBDefs schema definitions
- Type-safe querying with IntelliSense
- Performance-optimized binary reading

## Installation

```bash
# Core EF provider
dotnet add package MimironSQL.EntityFrameworkCore

# Providers (choose based on your needs)
dotnet add package MimironSQL.Providers.FileSystem

# Format support
dotnet add package MimironSQL.Formats.Wdc5
```

## Package Information

- **Package ID**: `MimironSQL.EntityFrameworkCore`
- **Target Framework**: .NET 10.0
- **Dependencies**:
  - `Microsoft.EntityFrameworkCore` 10.0.2
  - `Microsoft.EntityFrameworkCore.Proxies` 10.0.2
  - `Microsoft.EntityFrameworkCore.Relational` 10.0.2

## Quick Start

### Basic Configuration

```csharp
using Microsoft.EntityFrameworkCore;
using MimironSQL.EntityFrameworkCore;
using MimironSQL.Providers;

// Configure providers
var dbdProvider = new FileSystemDbdProvider(
    new FileSystemDbdProviderOptions(@"C:\WoWDBDefs\definitions"));

var db2Provider = new FileSystemDb2StreamProvider(
    new FileSystemDb2StreamProviderOptions(@"C:\WoW\DBFilesClient"));

var tactKeyProvider = new SimpleTactKeyProvider();

// Configure EF Core
var options = new DbContextOptionsBuilder<WoWDb2Context>()
    .UseMimironDb2(db2Provider, dbdProvider, tactKeyProvider)
    .Options;

using var context = new WoWDb2Context(options);
```

### Define Your DbContext

```csharp
public class WoWDb2Context : DbContext
{
    public WoWDb2Context(DbContextOptions<WoWDb2Context> options) : base(options) { }

    public DbSet<Map> Map => Set<Map>();
    public DbSet<MapChallengeMode> MapChallengeMode => Set<MapChallengeMode>();
    public DbSet<Spell> Spell => Set<Spell>();
    public DbSet<Item> Item => Set<Item>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        // Table mappings
        modelBuilder.Entity<Map>().ToTable("Map");
        modelBuilder.Entity<MapChallengeMode>().ToTable("MapChallengeMode");
        
        // Configure relationships
        modelBuilder.Entity<MapChallengeMode>()
            .HasOne(mc => mc.Map)
            .WithMany(m => m.MapChallengeModes)
            .HasForeignKey(mc => mc.MapID);
    }
}
```

## Public API Reference

### Configuration Extensions

#### `UseMimironDb2()`

Configures the DbContext to use the MimironSQL provider.

```csharp
public static DbContextOptionsBuilder UseMimironDb2(
    this DbContextOptionsBuilder optionsBuilder,
    IDb2StreamProvider db2Provider,
    IDbdProvider dbdProvider,
    ITactKeyProvider tactKeyProvider,
    Action<MimironDb2DbContextOptionsBuilder>? configureOptions = null)
```

**Parameters:**
- `db2Provider`: Provides access to DB2 file streams
- `dbdProvider`: Provides access to DBD schema definitions
- `tactKeyProvider`: Provides TACT encryption keys for encrypted sections
- `configureOptions`: Optional configuration callback

**Example:**
```csharp
var options = new DbContextOptionsBuilder<WoWDb2Context>()
    .UseMimironDb2(db2Provider, dbdProvider, tactKeyProvider)
    .Options;
```

#### Generic Overload

```csharp
public static DbContextOptionsBuilder<TContext> UseMimironDb2<TContext>(
    this DbContextOptionsBuilder<TContext> optionsBuilder,
    IDb2StreamProvider db2Provider,
    IDbdProvider dbdProvider,
    ITactKeyProvider tactKeyProvider,
    Action<MimironDb2DbContextOptionsBuilder>? configureOptions = null)
    where TContext : DbContext
```

### Service Registration

#### `AddMimironDb2FileSystem()`

Registers MimironSQL services with dependency injection for filesystem-based providers.

```csharp
public static IServiceCollection AddMimironDb2FileSystem(
    this IServiceCollection services,
    Action<MimironDb2FileSystemOptions> configure)
```

**Example:**
```csharp
services.AddMimironDb2FileSystem(options =>
{
    options.Db2DirectoryPath = @"C:\WoW\DBFilesClient";
    options.DbdDirectoryPath = @"C:\WoWDBDefs\definitions";
});

services.AddDbContext<WoWDb2Context>((serviceProvider, options) =>
{
    var db2Provider = serviceProvider.GetRequiredService<IDb2StreamProvider>();
    var dbdProvider = serviceProvider.GetRequiredService<IDbdProvider>();
    var tactKeyProvider = serviceProvider.GetRequiredService<ITactKeyProvider>();
    
    options.UseMimironDb2(db2Provider, dbdProvider, tactKeyProvider);
});
```

## Supported LINQ Operations

### Filtering

```csharp
// Where clause
var dungeonMaps = context.Map
    .Where(m => m.Directory!.Contains("dungeon"))
    .ToList();

// Complex predicates
var filtered = context.Spell
    .Where(s => s.Id > 1000 && s.Id < 2000)
    .Where(s => s.SpellName != null)
    .ToList();
```

### Projection

```csharp
// Anonymous types
var mapSummary = context.Map
    .Select(m => new { m.Id, m.MapName })
    .ToList();

// Named types
var dtos = context.Item
    .Select(i => new ItemDto 
    { 
        Id = i.Id, 
        Name = i.Name 
    })
    .ToList();

// Navigation projections
var withNav = context.MapChallengeMode
    .Select(mc => new 
    { 
        mc.Id, 
        MapName = mc.Map!.MapName 
    })
    .ToList();
```

### Navigation & Eager Loading

```csharp
// Single level Include
var modesWithMaps = context.MapChallengeMode
    .Include(mc => mc.Map)
    .ToList();

// Multiple includes
var spells = context.Spell
    .Include(s => s.SpellName)
    .Include(s => s.SpellMisc)
    .ToList();

// Nested includes (ThenInclude)
var deeply = context.Spell
    .Include(s => s.SpellName)
        .ThenInclude(sn => sn!.SpellDescription)
    .ToList();
```

### Limiting & Pagination

```csharp
// Take
var first10 = context.Map.Take(10).ToList();

// Skip + Take
var page2 = context.Item
    .Skip(50)
    .Take(50)
    .ToList();

// First/FirstOrDefault
var first = context.Map
    .Where(m => m.Id == 123)
    .FirstOrDefault();

// Single/SingleOrDefault
var single = context.Spell
    .Where(s => s.Id == 456)
    .SingleOrDefault();
```

### Aggregation

```csharp
// Count
var totalMaps = context.Map.Count();
var filteredCount = context.Map
    .Where(m => m.Directory!.Contains("raid"))
    .Count();

// Any
var hasResults = context.Spell
    .Any(s => s.Id > 10000);

// All
var allValid = context.Item
    .All(i => i.Id > 0);
```

## Configuration Examples

### Relationship Configuration

#### One-to-One (Shared Primary Key)

```csharp
modelBuilder.Entity<Spell>()
    .HasOne(s => s.SpellName)
    .WithOne(sn => sn.Spell)
    .HasForeignKey<SpellName>(sn => sn.Id);
```

#### Many-to-One

```csharp
modelBuilder.Entity<MapChallengeMode>()
    .HasOne(mc => mc.Map)
    .WithMany(m => m.MapChallengeModes)
    .HasForeignKey(mc => mc.MapID);
```

#### One-to-Many (Inverse of Many-to-One)

```csharp
modelBuilder.Entity<Map>()
    .HasMany(m => m.MapChallengeModes)
    .WithOne(mc => mc.Map)
    .HasForeignKey(mc => mc.MapID);
```

### Custom Table Names

```csharp
modelBuilder.Entity<MyCustomEntity>()
    .ToTable("ActualDb2TableName");
```

### Column Overrides

```csharp
modelBuilder.Entity<Spell>()
    .Property(s => s.SpellName)
    .HasColumnName("Name_lang");
```

### Using Configuration Classes

```csharp
public class MapConfiguration : IEntityTypeConfiguration<Map>
{
    public void Configure(EntityTypeBuilder<Map> builder)
    {
        builder.ToTable("Map");
        
        builder.HasKey(m => m.Id);
        
        builder.HasMany(m => m.MapChallengeModes)
            .WithOne(mc => mc.Map)
            .HasForeignKey(mc => mc.MapID);
    }
}

// In OnModelCreating:
protected override void OnModelCreating(ModelBuilder modelBuilder)
{
    modelBuilder.ApplyConfigurationsFromAssembly(Assembly.GetExecutingAssembly());
}
```

## Lazy Loading

Enable lazy loading with proxies:

```csharp
var options = new DbContextOptionsBuilder<WoWDb2Context>()
    .UseMimironDb2(db2Provider, dbdProvider, tactKeyProvider)
    .UseLazyLoadingProxies()  // Enable lazy loading
    .Options;
```

**Requirements for lazy loading:**
1. Navigation properties must be `virtual`
2. Entity classes must not be `sealed`
3. Navigation properties must have `get` and `set` accessors

```csharp
public class Map
{
    public int Id { get; set; }
    public string? MapName { get; set; }
    
    // Virtual for lazy loading
    public virtual ICollection<MapChallengeMode> MapChallengeModes { get; set; } = [];
}
```

## Change Tracking

The provider supports change tracking for query consistency:

```csharp
// Tracking enabled (default)
var options = new DbContextOptionsBuilder<WoWDb2Context>()
    .UseMimironDb2(db2Provider, dbdProvider, tactKeyProvider)
    .UseQueryTrackingBehavior(QueryTrackingBehavior.TrackAll)
    .Options;

// No tracking (better performance, no identity resolution)
var options = new DbContextOptionsBuilder<WoWDb2Context>()
    .UseMimironDb2(db2Provider, dbdProvider, tactKeyProvider)
    .UseQueryTrackingBehavior(QueryTrackingBehavior.NoTracking)
    .Options;
```

**Note**: Write operations (SaveChanges) are not supported regardless of tracking configuration.

## Schema Mapping

The provider automatically maps entity properties to DB2 columns using WoWDBDefs schema definitions:

- Entity type name → DB2 table name (case-insensitive)
- Property names → DBD column names (case-insensitive)
- .NET types mapped to DB2 types automatically

### Schema Validation

The provider validates that:
1. Entity properties match DBD column names
2. Property types are compatible with DBD column types
3. Foreign key properties exist in the schema

To skip validation for custom properties, use `[OverridesSchema]`:

```csharp
using MimironSQL;

public class MyEntity
{
    public int Id { get; set; }
    
    [OverridesSchema]
    public string? ComputedField { get; set; }  // Not validated against schema
}
```

## Performance Considerations

1. **Use NoTracking for read-only queries**:
   ```csharp
   var results = context.Map.AsNoTracking().ToList();
   ```

2. **Limit results early**:
   ```csharp
   var top100 = context.Spell.Take(100).ToList();
   ```

3. **Project only needed columns**:
   ```csharp
   var minimal = context.Map
       .Select(m => new { m.Id, m.MapName })
       .ToList();
   ```

4. **Use Include only when needed**:
   ```csharp
   // ❌ Always loads navigations
   var all = context.MapChallengeMode.Include(mc => mc.Map).ToList();
   
   // ✅ Only loads when needed
   var filtered = context.MapChallengeMode
       .Where(mc => mc.Id < 100)
       .Include(mc => mc.Map)
       .ToList();
   ```

## Limitations

### Not Supported

- ❌ Async queries (`ToListAsync`, `FirstOrDefaultAsync`, etc.)
- ❌ Write operations (`Add`, `Update`, `Remove`, `SaveChanges`)
- ❌ `GroupBy` operations
- ❌ Explicit `Join` operations (use navigation properties)
- ❌ Complex aggregations (`Sum`, `Average`, `Min`, `Max`)
- ❌ Database functions (`EF.Functions.*`)
- ❌ Raw SQL queries
- ❌ Migrations

### Workarounds

**For joins**: Use navigation properties with `Include`:
```csharp
// ❌ Not supported
var joined = context.MapChallengeMode
    .Join(context.Map, mc => mc.MapID, m => m.Id, (mc, m) => new { mc, m });

// ✅ Use Include
var included = context.MapChallengeMode
    .Include(mc => mc.Map)
    .ToList();
```

## Troubleshooting

### "No .db2 file found for table"

The provider couldn't find the DB2 file. Ensure:
- The file exists in the configured directory
- The file name matches the table name (case-insensitive)
- The file has a `.db2` extension

### "Field not found in schema"

A property on your entity doesn't match the DBD schema. Solutions:
- Check property name matches DBD column name exactly
- Use `HasColumnName()` to map to a different column
- Use `[OverridesSchema]` if it's a computed/custom property

### "Row type mismatch"

The file format doesn't match the expected row type. This is typically an internal error. Ensure you're using the correct format implementation (Wdc5Format for WDC5 files).

### Performance Issues

If queries are slow:
1. Use `Take()` to limit results
2. Use `AsNoTracking()` for read-only queries
3. Project only needed columns with `Select()`
4. Filter early with `Where()` clauses
5. Avoid loading entire tables without filtering

## Related Packages

- **MimironSQL.Contracts**: Core abstractions and interfaces
- **MimironSQL.Formats.Wdc5**: WDC5 format implementation
- **MimironSQL.Providers.FileSystem**: Filesystem providers
- **MimironSQL.DbContextGenerator**: Auto-generate DbContext from WoWDBDefs

## See Also

- [Root README](../../README.md)
- [Architecture Overview](../../.github/instructions/architecture.md)
- [Entity Framework Core Documentation](https://learn.microsoft.com/en-us/ef/core/)
