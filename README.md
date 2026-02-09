# MimironSQL

A read-only Entity Framework Core database provider for World of Warcraft DB2 files. MimironSQL lets you query game data tables using familiar LINQ and EF Core patterns, with full support for the WDC5 binary format and WoWDBDefs-driven schema discovery.

## Features

- **EF Core integration** — query DB2 files with LINQ, navigation properties, and the standard `DbContext` pattern
- **Source-generated DbContext** — a Roslyn source generator reads WoWDBDefs `.dbd` files at compile time and emits entity classes, configurations, and a typed `WoWDb2Context`
- **File system & CASC providers** — load DB2 data from extracted files on disk or directly from a World of Warcraft installation via CASC
- **Encrypted section support** — transparent Salsa20 decryption for TACT-encrypted DB2 sections
- **Extensible format & provider model** — implement `IDb2Format`, `IDb2StreamProvider`, `IDbdProvider`, `IDbdParser`, or `ITactKeyProvider` from the Contracts package to add new formats or data sources

## Packages

| Package | Description |
|---------|-------------|
| `MimironSQL.EntityFrameworkCore` | EF Core database provider for DB2 files |
| `MimironSQL.DbContextGenerator` | Source generator that emits entities and DbContext from `.dbd` definitions |
| `MimironSQL.Contracts` | Public interfaces and types for extending MimironSQL |
| `MimironSQL.Formats.Wdc5` | WDC5 binary format reader |
| `Salsa20` | Salsa20 stream cipher used for encrypted DB2 sections |

## Installation

Packages are published to **GitHub Packages**, not nuget.org. Add the GitHub NuGet source first:

```shell
dotnet nuget add source "https://nuget.pkg.github.com/Seriousnes/index.json" \
  --name MimironSQL \
  --username YOUR_GITHUB_USERNAME \
  --password YOUR_GITHUB_PAT
```

Then install the packages you need:

```shell
dotnet add package MimironSQL.EntityFrameworkCore
dotnet add package MimironSQL.DbContextGenerator
```

`MimironSQL.Contracts`, `MimironSQL.Formats.Wdc5`, and `Salsa20` are transitive dependencies — you only need to reference them directly if you're building a custom provider or format.

## Quick Start

### 1. Add a `.env` file

The source generator needs a WoW build version to select the correct DBD layout. Create a `.env` (or `.env.local`) file in your project root:

```
WOW_VERSION=12.0.0.65655
```

### 2. Define your DbContext

The `MimironSQL.DbContextGenerator` package generates a `WoWDb2Context` with `DbSet<T>` properties for every `.dbd` table definition. You can extend the generated context with a partial class:

```csharp
public partial class WoWDb2Context;
```

### 3. Register with DI

#### File system provider

```csharp
services.AddDbContext<WoWDb2Context>(options =>
    options.UseMimironDb2(o => o.UseFileSystem(
        db2DirectoryPath: "path/to/db2/files",
        dbdDefinitionsDirectory: "path/to/dbd/definitions")));
```

#### CASC provider

```csharp
services.AddDbContext<WoWDb2Context>(options =>
    options.UseMimironDb2(o => o.UseCascNet(
        wowInstallRoot: "path/to/World of Warcraft",
        dbdDefinitionsDirectory: "path/to/dbd/definitions")));
```

`UseCascNet` configures CASC for DB2 streams and uses the file system for `.dbd` definitions.

### 4. Query

```csharp
var maps = context.Maps
    .Where(m => m.MapType == 1)
    .ToList();
```

#### Notes
* **Read-only provider.** `SaveChanges()` throws `NotSupportedException`. Async query execution is not supported.

## Extensibility

The `MimironSQL.Contracts` package defines the extension points:

| Interface | Purpose |
|-----------|---------|
| `IDb2StreamProvider` | Opens a `Stream` for a named DB2 table |
| `IDbdProvider` | Opens a parsed `IDbdFile` for a named table |
| `IDbdParser` | Parses a `.dbd` stream or file path into an `IDbdFile` |
| `ITactKeyProvider` | Resolves TACT encryption keys by lookup ID |
| `IDb2Format` | Reads a DB2 binary stream into an `IDb2File` |

Implement any of these interfaces to plug in new data sources or file format versions.

If you're composing pieces outside of EF Core, you can register core services with:

```csharp
services.AddMimironSQLServices();
```

## Acknowledgments

This project builds on the work of the [wowdev](https://github.com/wowdev) community:

- [WoWDBDefs](https://github.com/wowdev/WoWDBDefs) — database definition files that provide column names, types, and layout metadata for DB2 tables
- [DBCD](https://github.com/wowdev/DBCD) — reference C# implementation for reading DB2 files
- [wowdev.wiki](https://wowdev.wiki/DB2) — community-maintained documentation of the DB2 file format specification

## License

[MIT](LICENSE.txt)
