# MimironSQL.EntityFrameworkCore

Read-only Entity Framework Core database provider for querying World of Warcraft DB2 files using standard EF Core patterns.

## Features

- Query DB2 files with LINQ and `DbContext` — no custom query language required.
- Schema resolution powered by [WoWDBDefs](https://github.com/wowdev/WoWDBDefs) `.dbd` definitions.
- Pluggable data providers — read from the local file system or directly from a CASC installation.
- Source-generated `DbContext` and entity classes via the companion `MimironSQL.DbContextGenerator` package.
- Built on EF Core 10 and targeting `net10.0`.

## Installation

Packages are published to GitHub Packages. Add the feed to your NuGet configuration, then install:

```shell
dotnet add package MimironSQL.EntityFrameworkCore
```

You also need at least one data provider package:

```shell
# File system provider
dotnet add package MimironSQL.Providers.FileSystem

# — or — CASC provider (reads from a WoW installation)
dotnet add package MimironSQL.Providers.CASC
```

> **Note:** See the [repository README](https://github.com/Seriousnes/MimironSQL) for GitHub Packages feed setup instructions.

## Getting Started

### 1. Add the source generator (recommended)

The `MimironSQL.DbContextGenerator` package generates entity classes and a typed `WoWDb2Context` from `.dbd` files at compile time:

```shell
dotnet add package MimironSQL.DbContextGenerator
```

Provide your `.env` and `.dbd` files as `AdditionalFiles` in your project:

```xml
<ItemGroup>
    <AdditionalFiles Include=".env" />
    <AdditionalFiles Include="path/to/WoWDBDefs/**/*.dbd" />
</ItemGroup>
```

The generator emits a `partial class WoWDb2Context : DbContext` that you can extend.

### 2. Configure the provider

Register the `DbContext` with one of the available providers.

**File system provider:**

```csharp
services.AddDbContext<WoWDb2Context>(options =>
    options.UseMimironDb2(o => o.UseFileSystem(
        db2DirectoryPath: "path/to/db2/files",
        dbdDefinitionsDirectory: "path/to/dbd/definitions")));
```

**CASC provider:**

```csharp
services.AddDbContext<WoWDb2Context>(options =>
    options.UseMimironDb2(o => o.UseCasc(configuration)));
```

Only one provider may be configured per `DbContext`.

## Querying

Standard LINQ queries work as expected:

```csharp
var dungeonMaps = context.Maps
    .Where(m => m.MapType == 2)
    .ToList();

var map = context.Maps.Find(2222);
```

## Limitations

- **Read-only.** `SaveChanges()` and `SaveChangesAsync()` throw `NotSupportedException`.
- **No async queries.** `ToListAsync()`, `FirstOrDefaultAsync()`, etc. throw `NotSupportedException`. Use synchronous equivalents.
- **No query precompilation.** Compiled queries are not supported.

## Configuration

Provider configuration is done through the `UseMimironDb2` extension method, which accepts an `Action<IMimironDb2DbContextOptionsBuilder>` callback. Inside the callback, call a provider-specific method (`UseFileSystem`, `UseCasc`, etc.) to select and configure the data source.

```csharp
options.UseMimironDb2(o =>
{
    // Exactly one provider method must be called here.
    o.UseFileSystem(db2Dir, dbdDir);
});
```

### Registering services without EF Core

If you need MimironSQL core services outside of a `DbContext`, register them manually:

```csharp
services.AddMimironSQLServices();
```

## Public API

### Extension Methods

```csharp
public static class MimironDb2DbContextOptionsExtensions
{
    public static DbContextOptionsBuilder UseMimironDb2(
        this DbContextOptionsBuilder optionsBuilder,
        Action<IMimironDb2DbContextOptionsBuilder> configureOptions);
}
```

### MimironDb2DbContextOptionsBuilder

```csharp
public class MimironDb2DbContextOptionsBuilder : IMimironDb2DbContextOptionsBuilder
{
    public MimironDb2DbContextOptionsBuilder(DbContextOptionsBuilder optionsBuilder);
    public DbContextOptionsBuilder OptionsBuilder { get; }
}
```

### MimironDb2OptionsExtension

Implements `IDbContextOptionsExtension`. Carries provider selection and registers all internal services during `ApplyServices`.

```csharp
public class MimironDb2OptionsExtension : IDbContextOptionsExtension
{
    public string? ProviderKey { get; }
    public int ProviderConfigHash { get; }
}
```

### Db2FieldSchema

Describes a single field in a resolved DB2 table schema:

```csharp
public readonly record struct Db2FieldSchema(
    string Name,
    Db2ValueType ValueType,
    int ColumnStartIndex,
    int ElementCount,
    bool IsVerified,
    bool IsVirtual,
    bool IsId,
    bool IsRelation,
    string? ReferencedTableName);
```

## Related Packages

| Package | Description |
|---------|-------------|
| [MimironSQL.Contracts](https://github.com/Seriousnes/MimironSQL) | Public interfaces and extension points |
| [MimironSQL.Providers.FileSystem](https://github.com/Seriousnes/MimironSQL) | File system-based DB2, DBD, and TACT key providers |
| [MimironSQL.Providers.CASC](https://github.com/Seriousnes/MimironSQL) | CASC-based DB2 provider (reads from a WoW installation) |
| [MimironSQL.DbContextGenerator](https://github.com/Seriousnes/MimironSQL) | Source generator for entity classes and typed DbContext |
| [MimironSQL.Formats.Wdc5](https://github.com/Seriousnes/MimironSQL) | WDC5 binary format reader |

## License

This project is licensed under the [MIT License](https://github.com/Seriousnes/MimironSQL/blob/main/LICENSE.txt).
