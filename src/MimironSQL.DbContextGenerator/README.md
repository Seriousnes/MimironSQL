# MimironSQL.DbContextGenerator

Roslyn incremental source generator that emits entity classes, EF Core configurations, and a typed `WoWDb2Context` from WoWDBDefs `.dbd` files at compile time.

## Features

- Parses `.dbd` definition files and resolves the build block matching your WoW version.
- Generates strongly-typed entity classes with `Id`, scalar properties, and virtual navigation stubs.
- Generates `IEntityTypeConfiguration<T>` implementations with table name, column mappings, and relationships.
- Generates a partial `WoWDb2Context` with `DbSet<T>` properties and `OnModelCreating` wiring.
- All generated types are `partial`, so you can extend them in your own source files.
- Ships as an analyzer/source-generator NuGet — no runtime dependency on this package.

## Installation

```shell
dotnet add package MimironSQL.DbContextGenerator
```

> The package is published to [GitHub Packages](https://github.com/Seriousnes/MimironSQL/packages). Configure a NuGet source for `https://nuget.pkg.github.com/Seriousnes/index.json` if you haven't already.

## Getting Started

### 1. Provide `.dbd` files via `AdditionalFiles`

This package does not download WoWDBDefs automatically. Obtain the `.dbd` files however you like (script, git submodule, checked-in snapshot, etc.) and reference them as `AdditionalFiles` alongside a `.env` configuration file:

```xml
<ItemGroup>
    <AdditionalFiles Include=".env" />
    <AdditionalFiles Include="path/to/WoWDBDefs/**/*.dbd" />
</ItemGroup>
```

### 2. Add a `.env` file

Create a `.env` (or `.env.local`) file in your project root with the target WoW build version:

```
WOW_VERSION=12.0.0.65655
```

The generator uses this version to select the matching DBD build block for each table.

### 3. Declare the partial context

Add a partial class declaration so the generator can extend it:

```csharp
public partial class WoWDb2Context;
```

Build the project. The generator will run at compile time and emit the source files described below.

## What Gets Generated

| Generated File | Description |
|---|---|
| `WoWDb2Context.g.cs` | Partial `DbContext` with `DbSet<T>` properties and `OnModelCreating` wiring for every resolved table. |
| `{Entity}.g.cs` | Entity class per table containing an `Id` property, scalar properties, and virtual navigation properties. |
| `{Entity}Configuration.g.cs` | `IEntityTypeConfiguration<T>` with table name, column mappings, and relationship configuration. |

All generated types are emitted as `partial` classes, allowing you to add members or override behavior without modifying generated code.

## Extending Generated Types

Add navigations, computed properties, or additional configuration in your own partial files:

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
|---|---|---|
| `MSQLDBD004` | Warning | No sources were generated (missing `.env`/`.dbd`, invalid `WOW_VERSION`, or no compatible build blocks). |

## Requirements

- The generator targets `netstandard2.0` and works with any SDK-style project on .NET 6+.
- WoWDBDefs `.dbd` files must exist on disk at build time and be included as `AdditionalFiles`.
- A `.env` or `.env.local` file specifying `WOW_VERSION` must also be included as an `AdditionalFile`.

## License

This project is licensed under the [MIT License](../../LICENSE.txt).
