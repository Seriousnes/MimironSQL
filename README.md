# MimironSQL

A high-performance, read-only Entity Framework Core (EF Core) database provider for querying World of Warcraft DB2 files. Built on top of the WoWDBDefs schema definitions, MimironSQL enables developers to query WoW game data using standard LINQ expressions and the familiar EF Core API.

## Features

- **EF Core Integration**: Query DB2 files using standard Entity Framework Core LINQ queries
- **Multiple Format Support**: Currently supports WDC5 format with extensible architecture for future formats
- **Flexible Data Sources**: Access DB2 files from filesystem or CASC (Content Addressable Storage Container)
- **Relationship Navigation**: Support for `Include` and `ThenInclude` to eagerly load related entities
- **Source Generator**: Automatic DbContext generation from WoWDBDefs definitions
- **Type-Safe Queries**: Full IntelliSense support with strongly-typed entity classes
- **Performance Optimized**: Lazy evaluation and efficient binary parsing

## Quick Start

### Prerequisites

- .NET 10.0 or later
- WoWDBDefs definition files (clone from [WoWDBDefs repository](https://github.com/wowdev/WoWDBDefs))
- World of Warcraft DB2 files (extract from your WoW installation)

### Installation

Install the required NuGet packages:

```bash
# Core EF provider
dotnet add package MimironSQL.EntityFrameworkCore

# Filesystem provider for reading DB2/DBD files from disk
dotnet add package MimironSQL.Providers.FileSystem

# Source generator for auto-generating DbContext (optional but recommended)
dotnet add package MimironSQL.DbContextGenerator

# WDC5 format support
dotnet add package MimironSQL.Formats.Wdc5
```

### Basic Usage

#### 1. Configure the EF Core Provider

```csharp
using Microsoft.EntityFrameworkCore;
using MimironSQL.EntityFrameworkCore;
using MimironSQL.Providers;

// Configure providers for DB2 files and DBD definitions
var dbdProvider = new FileSystemDbdProvider(
    new FileSystemDbdProviderOptions(@"C:\path\to\WoWDBDefs\definitions"));

var db2Provider = new FileSystemDb2StreamProvider(
    new FileSystemDb2StreamProviderOptions(@"C:\path\to\wow\DBFilesClient"));

// Create a simple TACT key provider (for encrypted sections)
var tactKeyProvider = new SimpleTactKeyProvider();

// Configure DbContext
var options = new DbContextOptionsBuilder<WoWDb2Context>()
    .UseMimironDb2(db2Provider, dbdProvider, tactKeyProvider)
    .Options;

using var context = new WoWDb2Context(options);
```

#### 2. Define Your DbContext (Manual Approach)

```csharp
public class WoWDb2Context : DbContext
{
    public WoWDb2Context(DbContextOptions<WoWDb2Context> options) 
        : base(options) { }

    public DbSet<Map> Map => Set<Map>();
    public DbSet<MapChallengeMode> MapChallengeMode => Set<MapChallengeMode>();
    public DbSet<Spell> Spell => Set<Spell>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        // Configure table mappings
        modelBuilder.Entity<Map>().ToTable("Map");
        modelBuilder.Entity<MapChallengeMode>().ToTable("MapChallengeMode");
        
        // Configure relationships
        modelBuilder.Entity<MapChallengeMode>()
            .HasOne(mc => mc.Map)
            .WithMany(m => m.MapChallengeModes)
            .HasForeignKey(mc => mc.MapID);
    }
}

public class Map
{
    public int Id { get; set; }
    public string? Directory { get; set; }
    public string? MapName { get; set; }
    
    public virtual ICollection<MapChallengeMode> MapChallengeModes { get; set; } = [];
}

public class MapChallengeMode
{
    public int Id { get; set; }
    public int MapID { get; set; }
    
    public virtual Map? Map { get; set; }
}
```

#### 3. Query Using LINQ

```csharp
// Simple query
var maps = context.Map
    .Where(m => m.Directory!.Contains("dungeon"))
    .Take(10)
    .ToList();

// Query with navigation
var challengeModes = context.MapChallengeMode
    .Include(mc => mc.Map)
    .Where(mc => mc.Map != null && mc.Map.MapName!.StartsWith("The"))
    .ToList();

// Nested includes
var spells = context.Spell
    .Include(s => s.SpellName)
    .ThenInclude(sn => sn.SpellDescription)
    .Take(100)
    .ToList();

// Projections
var mapSummary = context.Map
    .Select(m => new { m.Id, m.MapName, m.Directory })
    .Take(50)
    .ToList();
```

### Using the Source Generator (Recommended)

The `MimironSQL.DbContextGenerator` automatically generates your DbContext and entity classes from WoWDBDefs:

1. Add the generator package:
   ```bash
   dotnet add package MimironSQL.DbContextGenerator
   ```

2. Create a `.env` file in your project root:
   ```env
   WOW_VERSION=11.0.7.58162
   DBD_PATH=C:\path\to\WoWDBDefs\definitions
   ```

3. Add the `.env` file to your `.csproj`:
   ```xml
   <ItemGroup>
     <AdditionalFiles Include=".env" />
   </ItemGroup>
   ```

4. The generator will create your DbContext automatically at compile time!

## Repository Structure

```
MimironSQL/
├── src/
│   ├── MimironSQL.Contracts          # Public interfaces and abstractions for extensibility
│   ├── MimironSQL.EntityFrameworkCore # EF Core database provider implementation
│   ├── MimironSQL.Formats.Wdc5       # WDC5 format reader and parser
│   ├── MimironSQL.DbContextGenerator # Roslyn source generator for DbContext
│   ├── MimironSQL.Providers.FileSystem # Filesystem-based DB2/DBD providers
│   ├── MimironSQL.Providers.CASC     # CASC-based DB2/DBD providers (WIP)
│   ├── MimironSQL.Dbd                # DBD file parser (WoWDBDefs format)
│   ├── Salsa20                       # Salsa20 encryption for encrypted DB2 sections
│   └── MimironSQL.Benchmarks         # Performance benchmarking suite
└── tests/                            # Unit and integration tests
```

Each project under `src/` has its own comprehensive README explaining its public API and usage.

## Supported LINQ Operations

MimironSQL supports a rich subset of LINQ operations:

### Filtering & Projection
- ✅ `Where` - Filter records
- ✅ `Select` - Project to new types (including anonymous types)
- ✅ `FirstOrDefault`, `First` - Get first element
- ✅ `SingleOrDefault`, `Single` - Get single element

### Navigation & Eager Loading
- ✅ `Include` - Eager load related entities
- ✅ `ThenInclude` - Eager load nested relationships

### Limiting & Pagination
- ✅ `Take` - Limit results
- ✅ `Skip` - Skip records for pagination

### Aggregation
- ✅ `Count` - Count records
- ✅ `Any` - Check if any records exist
- ✅ `All` - Check if all records match predicate

### Not Supported
- ❌ Async operations (`ToListAsync`, etc.)
- ❌ Write operations (Insert, Update, Delete, SaveChanges)
- ❌ Grouping (`GroupBy`)
- ❌ Joins (use navigation properties with `Include` instead)
- ❌ Complex aggregations (`Sum`, `Average`, `Min`, `Max`)

## Architecture Overview

```
LINQ Query
    ↓
EF Core Query Provider
    ↓
MimironSQL Query Pipeline
    ↓
Schema Mapper (WoWDBDefs)
    ↓
Format Reader (WDC5)
    ↓
Stream Provider (FileSystem/CASC)
```

The architecture is designed to be extensible:

- **Formats**: Implement `IDb2Format` to support new DB2 formats (WDC4, WDC6, etc.)
- **Providers**: Implement `IDb2StreamProvider` for new data sources (network, archives, etc.)
- **Schema**: Implement `IDbdProvider` for alternative schema sources

See [architecture.md](./.github/instructions/architecture.md) for detailed design documentation.

## Performance Tips

1. **Filter Early**: Apply `Where` clauses early in your query chain
2. **Limit Results**: Use `Take()` to avoid materializing entire tables
3. **Project Selectively**: Use `Select()` to project only needed columns
4. **Use Include Sparingly**: Only eager-load navigations when actually needed
5. **Cache DbContext**: Reuse the same DbContext instance for multiple queries in a session

## Examples

### Example 1: Find Maps by Name Pattern

```csharp
var dungeonMaps = context.Map
    .Where(m => m.Directory!.Contains("dungeon"))
    .Select(m => new { m.Id, m.MapName, m.Directory })
    .ToList();
```

### Example 2: Query with Multiple Navigations

```csharp
var spellsWithDetails = context.Spell
    .Include(s => s.SpellName)
    .Include(s => s.SpellMisc)
    .Where(s => s.SpellName != null)
    .Take(100)
    .ToList();
```

### Example 3: Pagination

```csharp
int pageSize = 50;
int pageNumber = 2;

var pagedResults = context.Item
    .OrderBy(i => i.Id)
    .Skip(pageSize * (pageNumber - 1))
    .Take(pageSize)
    .ToList();
```

## Troubleshooting

### "No .db2 file found for table"
Ensure your DB2 files directory contains the required `.db2` files and the table name matches the file name (case-insensitive).

### "Field not found in schema"
Your entity property names must match the field names in the corresponding `.dbd` definition file from WoWDBDefs. Check the definition for exact field names.

### "Entity type has no key member"
Ensure your entity class:
1. Has a public primary key configured by convention (e.g., a property named `Id`), OR
2. Has a key configured via `modelBuilder.Entity<T>().HasKey(e => e.CustomId)`, OR
3. Is configured as keyless via `modelBuilder.Entity<T>().HasNoKey()` (if appropriate)

### Navigation Not Working
1. Check that the navigation and FK are configured in `OnModelCreating`
2. Verify FK and PK members map to DB2 columns in the `.dbd` schema
3. If using lazy-loading proxies, ensure navigation properties are `virtual`

## Development

### Building the Project

```bash
# Build all projects
dotnet build MimironSQL.slnx

# Build in Release mode
dotnet build MimironSQL.slnx -c Release
```

### Running Tests

```bash
# Run all tests
dotnet test MimironSQL.slnx

# Run tests with coverage
dotnet test MimironSQL.slnx --collect:"XPlat Code Coverage"
```

### Code Coverage

See [tools/coverage/README.md](./tools/coverage/README.md) for detailed instructions on generating and analyzing code coverage reports.

## Contributing

Contributions are welcome! Please:

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Make your changes following the [coding style guidelines](./.github/instructions/coding-style.md)
4. Write tests for your changes
5. Ensure all tests pass (`dotnet test MimironSQL.slnx`)
6. Submit a pull request

See [test-strategy.md](./.github/instructions/test-strategy.md) for testing guidelines.

## License

This project is licensed under the MIT License. See [LICENSE.txt](LICENSE.txt) for details.

## Acknowledgments

For acknowledgments of specifications and community resources used in this project, see [THIRD_PARTY_NOTICES.md](THIRD_PARTY_NOTICES.md).

Special thanks to:
- The [WoWDBDefs](https://github.com/wowdev/WoWDBDefs) community for maintaining DB2 schema definitions
- The [wowdev.wiki](https://wowdev.wiki) community for DB2 format documentation

## Related Resources

- [WoWDBDefs Repository](https://github.com/wowdev/WoWDBDefs) - DB2 schema definitions
- [wowdev.wiki - DB2](https://wowdev.wiki/DB2) - DB2 file format specification
- [Entity Framework Core Documentation](https://learn.microsoft.com/en-us/ef/core/) - EF Core reference
