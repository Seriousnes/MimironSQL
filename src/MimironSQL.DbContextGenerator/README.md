# MimironSQL.DbContextGenerator

Roslyn source generator that automatically generates strongly-typed EF Core DbContext and entity classes from WoWDBDefs definitions. Eliminates manual entity class creation and ensures your code stays in sync with DB2 schema changes.

## Overview

`MimironSQL.DbContextGenerator` is a compile-time source generator that:

- Generates DbContext with `DbSet<T>` properties for all DB2 tables
- Creates entity classes based on DBD schema definitions
- Configures relationships and foreign keys automatically
- Handles array properties and special types
- Provides compile-time safety and IntelliSense support
- Updates automatically when DBD definitions change

## Installation

```bash
dotnet add package MimironSQL.DbContextGenerator
```

## Package Information

- **Package ID**: `MimironSQL.DbContextGenerator`
- **Target Framework**: .NET Standard 2.0 (Roslyn analyzer)
- **Type**: Roslyn Incremental Source Generator
- **Dependencies**:
  - `Microsoft.CodeAnalysis.CSharp` 4.12.0
  - `MimironSQL.Contracts`
  - `MimironSQL.Dbd`

## Quick Start

### 1. Add Generator Package

```bash
dotnet add package MimironSQL.DbContextGenerator
```

### 2. Create `.env` Configuration File

Create a `.env` file in your project root:

```env
WOW_VERSION=11.0.7.58162
DBD_PATH=C:\path\to\WoWDBDefs\definitions
```

**Configuration:**
- `WOW_VERSION`: WoW build version (format: `major.minor.patch` or `major.minor.patch.build`)
- `DBD_PATH`: Path to WoWDBDefs definitions directory

### 3. Add `.env` to Project File

Edit your `.csproj`:

```xml
<Project Sdk="Microsoft.NET.Sdk">
  <PropertyGroup>
    <TargetFramework>net10.0</TargetFramework>
  </PropertyGroup>

  <ItemGroup>
    <PackageReference Include="MimironSQL.DbContextGenerator" />
    <PackageReference Include="MimironSQL.EntityFrameworkCore" />
  </ItemGroup>

  <ItemGroup>
    <!-- Make .env file available to the generator -->
    <AdditionalFiles Include=".env" />
  </ItemGroup>
</Project>
```

### 4. Build Project

```bash
dotnet build
```

The generator will create:
- `WoWDb2Context` class (DbContext)
- Entity classes for each DB2 table (e.g., `Map`, `Spell`, `Item`)
- Relationship configurations

### 5. Use Generated Code

```csharp
using Microsoft.EntityFrameworkCore;
using MimironSQL.EntityFrameworkCore;

var options = new DbContextOptionsBuilder<WoWDb2Context>()
    .UseMimironDb2(db2Provider, dbdProvider, tactKeyProvider)
    .Options;

using var context = new WoWDb2Context(options);

// All DbSets are available
var maps = context.Map.Take(10).ToList();
var spells = context.Spell.Include(s => s.SpellName).ToList();
```

## Generated Code Structure

### Generated DbContext

```csharp
// Generated code
public partial class WoWDb2Context : DbContext
{
    public WoWDb2Context(DbContextOptions<WoWDb2Context> options) 
        : base(options) { }

    // DbSet properties for each table
    public DbSet<Map> Map => Set<Map>();
    public DbSet<Spell> Spell => Set<Spell>();
    public DbSet<Item> Item => Set<Item>();
    // ... more tables

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        // Table configurations
        modelBuilder.Entity<Map>().ToTable("Map");
        modelBuilder.Entity<Spell>().ToTable("Spell");
        
        // Relationship configurations
        modelBuilder.Entity<MapChallengeMode>()
            .HasOne(p => p.Map)
            .WithMany()
            .HasForeignKey(p => p.MapID);
        
        // ... more configurations
    }
}
```

### Generated Entity Classes

```csharp
// Generated entity for Map table
public partial class Map : Db2Entity
{
    public required string Directory { get; set; }
    public required string MapName { get; set; }
    public required string MapDescription0 { get; set; }
    public required string MapDescription1 { get; set; }
    // ... more properties

    // Navigation properties
    public virtual ICollection<MapChallengeMode> MapChallengeModes { get; set; } = [];
}

// Generated entity for Spell table  
public partial class Spell : Db2Entity
{
    public required string Name { get; set; }
    public required string NameSubtext { get; set; }
    public required string Description { get; set; }
    // ... more properties
    
    // Navigation property
    public virtual SpellName? SpellName { get; set; }
}
```

## Configuration

### `.env` File Format

```env
# Required: WoW build version
WOW_VERSION=11.0.7.58162

# Required: Path to WoWDBDefs definitions directory
DBD_PATH=C:\path\to\WoWDBDefs\definitions

# Optional: Customize generated context name (default: WoWDb2Context)
# CONTEXT_NAME=MyCustomContext

# Optional: Namespace for generated code (default: project's root namespace)
# NAMESPACE=MyProject.Data
```

### Supported Version Formats

- `11.0.7` (major.minor.patch)
- `11.0.7.58162` (major.minor.patch.build)

The generator selects the appropriate DBD layout based on the specified version.

## Entity Base Classes

Generated entities inherit from base classes based on their ID field:

### `Db2Entity` (int Id)

For entities with a standard `int` ID field:

```csharp
public abstract class Db2Entity
{
    public int Id { get; set; }
}

public partial class Map : Db2Entity
{
    // Id is inherited
    public required string MapName { get; set; }
}
```

### `Db2EntityUInt` (uint Id)

For entities with a `uint` ID field:

```csharp
public abstract class Db2EntityUInt
{
    public uint Id { get; set; }
}

public partial class Achievement : Db2EntityUInt
{
    // Id is inherited
    public required string Title { get; set; }
}
```

### `Db2EntityULong` (ulong Id)

For entities with a `ulong` ID field:

```csharp
public abstract class Db2EntityULong
{
    public ulong Id { get; set; }
}

public partial class SpecialTable : Db2EntityULong
{
    // Id is inherited
    public required string Data { get; set; }
}
```

## Type Mappings

The generator maps DBD types to .NET types:

| DBD Type | .NET Type | Notes |
|----------|-----------|-------|
| `int` | `int` | Signed 32-bit |
| `uint` | `uint` | Unsigned 32-bit |
| `long` | `long` | Signed 64-bit |
| `ulong` | `ulong` | Unsigned 64-bit |
| `float` | `float` | Single-precision |
| `string` | `string` | Nullable reference |
| `locstring` | `string` | Localized string |
| `int[]` | `int[]` | Array property |
| `uint<EnumName>` | `EnumName` | Enum type |

## Array Properties

Array columns in DBD are generated as array properties:

```csharp
// DBD: int[3] Position
public int[] Position { get; set; } = new int[3];

// DBD: float[4] Quaternion
public float[] Quaternion { get; set; } = new float[4];
```

## Relationship Generation

### Foreign Key Detection

The generator automatically creates navigation properties for foreign keys:

```csharp
// DBD: column MapID references Map.ID
public partial class MapChallengeMode : Db2Entity
{
    public int MapID { get; set; }
    
    // Generated navigation
    public virtual Map? Map { get; set; }
}
```

### Collection Navigation

Inverse navigations are generated on the target entity:

```csharp
public partial class Map : Db2Entity
{
    // Generated inverse navigation
    public virtual ICollection<MapChallengeMode> MapChallengeModes { get; set; } = [];
}
```

### Configuration

Relationships are configured in `OnModelCreating`:

```csharp
modelBuilder.Entity<MapChallengeMode>()
    .HasOne(mc => mc.Map)
    .WithMany(m => m.MapChallengeModes)
    .HasForeignKey(mc => mc.MapID);
```

## Customization

### Partial Classes

All generated entities are `partial` classes, allowing you to extend them:

```csharp
// Your custom code file
public partial class Map
{
    // Add custom properties
    public string DisplayName => MapName ?? "Unknown";
    
    // Add custom methods
    public bool IsDungeon() => Directory?.Contains("dungeon") ?? false;
    
    // Add computed properties
    public int ChallengeModeCount => MapChallengeModes?.Count ?? 0;
}
```

### Partial DbContext

The DbContext is also partial:

```csharp
// Your custom code file
public partial class WoWDb2Context
{
    // Add custom DbSets
    public DbSet<CustomView> CustomViews => Set<CustomView>();
    
    // Add custom configuration
    partial void OnModelCreatingPartial(ModelBuilder modelBuilder)
    {
        // Your custom model configuration
        modelBuilder.Entity<Map>()
            .HasQueryFilter(m => m.MapName != null);
    }
}
```

## Diagnostics

The generator provides helpful diagnostics:

### MSQLDBD001: Missing .env file

```
Error: MimironSQL.DbContextGenerator requires a .env file provided via AdditionalFiles
```

**Solution:** Add `.env` file and include it in `<AdditionalFiles>` in your `.csproj`.

### MSQLDBD002: Missing WOW_VERSION

```
Error: MimironSQL.DbContextGenerator requires WOW_VERSION=... in .env
```

**Solution:** Add `WOW_VERSION=11.0.7.58162` to your `.env` file.

### MSQLDBD003: Invalid WOW_VERSION

```
Error: WOW_VERSION value 'invalid' could not be parsed. Expected 'major.minor.patch' or 'major.minor.patch.build'.
```

**Solution:** Use correct version format like `11.0.7` or `11.0.7.58162`.

## Performance

The generator uses incremental generation for fast builds:

- **First build**: Parses all DBD files (~200ms for ~500 tables)
- **Subsequent builds**: Only regenerates if `.env` or DBD files change (~10ms)
- **IntelliSense**: Updates as you type with minimal overhead

## Troubleshooting

### Generator Not Running

1. Ensure `.env` is in `<AdditionalFiles>`:
   ```xml
   <AdditionalFiles Include=".env" />
   ```

2. Clean and rebuild:
   ```bash
   dotnet clean
   dotnet build
   ```

3. Check generator is referenced:
   ```bash
   dotnet list package --include-transitive | grep DbContextGenerator
   ```

### Generated Code Not Visible

1. Check for build errors in the Error List
2. Look for generated files in `obj/Debug/net10.0/generated/`
3. Ensure project targets compatible framework (.NET 10.0+)

### Wrong Version Layout

If generated entities don't match your DB2 files:

1. Verify `WOW_VERSION` matches your WoW client version
2. Update WoWDBDefs to latest definitions
3. Clean and rebuild project

### Relationship Not Generated

If a foreign key relationship isn't generated:

1. Check DBD file has the relationship defined
2. Verify both tables are included in generation
3. Ensure foreign key column exists in the DBD layout

## Best Practices

1. **Version Control `.env`**: Commit your `.env` file to ensure team members use the same configuration

2. **Update WoWDBDefs Regularly**: Keep your DBD definitions up to date for new patches

3. **Use Partial Classes**: Extend generated entities with custom logic using partial classes

4. **Don't Edit Generated Code**: Never manually edit generated files - they'll be overwritten

5. **Configure in OnModelCreating**: Use the partial `OnModelCreatingPartial` method for custom configurations

## Advanced Scenarios

### Multiple Contexts

Generate multiple contexts for different WoW versions:

```xml
<ItemGroup>
  <AdditionalFiles Include=".env.retail" />
  <AdditionalFiles Include=".env.classic" />
</ItemGroup>
```

Configure each `.env` file with different versions and use separate namespaces.

### Custom Entity Configuration

```csharp
public partial class WoWDb2Context
{
    partial void OnModelCreatingPartial(ModelBuilder modelBuilder)
    {
        // Add indexes
        modelBuilder.Entity<Map>()
            .HasIndex(m => m.MapName);
        
        // Add query filters
        modelBuilder.Entity<Item>()
            .HasQueryFilter(i => i.Id > 0);
        
        // Configure value converters
        modelBuilder.Entity<Spell>()
            .Property(s => s.SpellMiscID)
            .HasConversion<string>();
    }
}
```

## Related Packages

- **MimironSQL.EntityFrameworkCore**: EF Core provider
- **MimironSQL.Contracts**: Core abstractions
- **MimironSQL.Dbd**: DBD file parser

## See Also

- [Root README](../../README.md)
- [WoWDBDefs Repository](https://github.com/wowdev/WoWDBDefs)
- [Roslyn Source Generators](https://learn.microsoft.com/en-us/dotnet/csharp/roslyn-sdk/source-generators-overview)
