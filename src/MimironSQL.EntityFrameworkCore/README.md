# MimironSQL.EntityFrameworkCore

Read-only Entity Framework Core database provider for World of Warcraft DB2 files.

## Setup

```csharp
services.AddDbContext<WoWDb2Context>(options =>
    options.UseMimironDb2(db2StreamProvider, dbdProvider, tactKeyProvider));
```

`UseMimironDb2` requires three provider instances:

| Parameter | Type | Purpose |
|-----------|------|---------|
| `db2StreamProvider` | `IDb2StreamProvider` | Opens raw DB2 byte streams |
| `dbdProvider` | `IDbdProvider` | Provides parsed DBD schema metadata |
| `tactKeyProvider` | `ITactKeyProvider` | Resolves TACT encryption keys |

See `MimironSQL.Providers.FileSystem` and `MimironSQL.Providers.CASC` for built-in implementations.

An optional `Action<MimironDb2DbContextOptionsBuilder>` callback is accepted for future configuration extensibility.

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

## Public API

### Extension Methods

```csharp
public static class MimironDb2DbContextOptionsExtensions
{
    public static DbContextOptionsBuilder UseMimironDb2(
        this DbContextOptionsBuilder optionsBuilder,
        IDb2StreamProvider db2Provider,
        IDbdProvider dbdProvider,
        ITactKeyProvider tactKeyProvider,
        Action<MimironDb2DbContextOptionsBuilder>? configureOptions = null);
}
```

### `MimironDb2DbContextOptionsBuilder`

```csharp
public class MimironDb2DbContextOptionsBuilder
{
    public MimironDb2DbContextOptionsBuilder(DbContextOptionsBuilder optionsBuilder);
}
```

### `MimironDb2OptionsExtension`

Implements `IDbContextOptionsExtension`. Carries the three provider instances and registers all internal services during `ApplyServices`.

```csharp
public class MimironDb2OptionsExtension : IDbContextOptionsExtension
{
    public IDb2StreamProvider? Db2StreamProvider { get; }
    public IDbdProvider? DbdProvider { get; }
    public ITactKeyProvider? TactKeyProvider { get; }

    public MimironDb2OptionsExtension WithProviders(
        IDb2StreamProvider db2StreamProvider,
        IDbdProvider dbdProvider,
        ITactKeyProvider tactKeyProvider);
}
```

### `Db2FieldSchema`

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
