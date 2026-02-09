# MimironSQL.EntityFrameworkCore

Read-only Entity Framework Core database provider for World of Warcraft DB2 files.

## Setup

```csharp
services.AddDbContext<WoWDb2Context>(options =>
    options.UseMimironDb2(o => o.UseFileSystem(
        db2DirectoryPath: "path/to/db2/files",
        dbdDefinitionsDirectory: "path/to/dbd/definitions")));
```

Provider configuration is done via the `Action<MimironDb2DbContextOptionsBuilder>` callback. Provider-specific extension methods live in their provider packages (for example, `UseFileSystem(...)` and `UseCascNet(...)`). Only one provider may be configured.

If you need to register core MimironSQL services outside of EF Core, you can use:

```csharp
services.AddMimironSQLServices();
```

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
    Action<MimironDb2DbContextOptionsBuilder> configureOptions);
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

Implements `IDbContextOptionsExtension`. Carries provider selection and registers all internal services during `ApplyServices`.

```csharp
public class MimironDb2OptionsExtension : IDbContextOptionsExtension
{
    public string? ProviderKey { get; }
    public int ProviderConfigHash { get; }
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
