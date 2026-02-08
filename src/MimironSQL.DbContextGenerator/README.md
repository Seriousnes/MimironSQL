# MimironSQL.DbContextGenerator

Roslyn incremental source generator that emits entity classes, EF Core configurations, and a typed `WoWDb2Context` from WoWDBDefs `.dbd` files at compile time.

## Setup

### 1. Install the package

```shell
dotnet add package MimironSQL.DbContextGenerator
```

The package includes MSBuild targets that automatically download WoWDBDefs definitions to `%LOCALAPPDATA%\MimironSQL\wowdbdefs` on first build. Override the cache location with:

```xml
<PropertyGroup>
  <MimironSqlWowDbDefsRoot>path/to/custom/cache</MimironSqlWowDbDefsRoot>
</PropertyGroup>
```

### 2. Add a `.env` file

Create a `.env` file in your project root with the target WoW build version:

```
WOW_VERSION=12.0.0.65655
```

The generator uses this version to select the matching DBD build block for each table.

### 3. Declare the partial context

```csharp
public partial class WoWDb2Context;
```

The generator emits:
- **`WoWDb2Context.g.cs`** — partial `DbContext` with `DbSet<T>` properties and `OnModelCreating` wiring
- **`{Entity}.g.cs`** — entity class per table with `Id`, scalar properties, and virtual navigation properties
- **`{Entity}Configuration.g.cs`** — `IEntityTypeConfiguration<T>` with table name, column mappings, and relationships

## Extending Generated Types

Both entities and configurations are generated as `partial` classes. Add navigations, computed properties, or additional configuration in your own partial files:

```csharp
// Map.cs
public partial class Map
{
    public virtual ICollection<MapChallengeMode> MapChallengeModes { get; set; }
}

// MapChallengeModeConfiguration.cs
public partial class MapChallengeModeConfiguration
{
    partial void ConfigureNavigation(EntityTypeBuilder<MapChallengeMode> builder)
    {
        builder.HasOne(x => x.Map)
            .WithMany(x => x.MapChallengeModes)
            .HasForeignKey(x => x.MapId);
    }
}
```

## Diagnostics

| Code | Severity | Description |
|------|----------|-------------|
| `MSQLDBD001` | Error | Missing `.env` file in project |
| `MSQLDBD002` | Error | `.env` file does not contain `WOW_VERSION` |
| `MSQLDBD003` | Error | `WOW_VERSION` value is not a valid version string |
