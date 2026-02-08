# MimironSQL

MimironSQL is a read-only Entity Framework Core (EF Core) database provider for querying World of Warcraft DB2/WDC5 data.

The legacy custom query surface (`Db2Context` / `Db2Table`) has been removed (issue #43). The supported public surface is the EF provider.

## Getting Started

### 1) Provide access to DB2 + DBD

```csharp
using Microsoft.EntityFrameworkCore;

using MimironSQL.EntityFrameworkCore;
using MimironSQL.Providers;

using NSubstitute;

var dbdProvider = new FileSystemDbdProvider(new FileSystemDbdProviderOptions(@"C:\path\to\WoWDBDefs"));
var db2Provider = new FileSystemDb2StreamProvider(new FileSystemDb2StreamProviderOptions(@"C:\path\to\DBFilesClient"));

var tactKeyProvider = Substitute.For<ITactKeyProvider>();
tactKeyProvider.TryGetKey(Arg.Any<ulong>(), out Arg.Any<ReadOnlyMemory<byte>>()).Returns(false);

var options = new DbContextOptionsBuilder<WoWDb2Context>()
    .UseMimironDb2(db2Provider, dbdProvider, tactKeyProvider)
    // Optional:
    // .UseLazyLoadingProxies()
    .Options;

using var context = new WoWDb2Context(options);
```

### 2) Query using EF Core

```csharp
var results = context.MapChallengeMode
    .Include(x => x.Map)
    .Where(x => x.Map != null && x.Map.Directory.Contains('a'))
    .Take(50)
    .ToList();
```

## Repository Layout

- [src/MimironSQL.EntityFrameworkCore](src/MimironSQL.EntityFrameworkCore) — EF Core provider implementation
- [src/MimironSQL.DbContextGenerator](src/MimironSQL.DbContextGenerator) — source generator for EF `DbContext` + mappings
- [src/MimironSQL.Providers.FileSystem](src/MimironSQL.Providers.FileSystem) — filesystem providers
- [src/MimironSQL.Providers.CASC](src/MimironSQL.Providers.CASC) — CASC providers

Each project under `src/` has its own README with public API notes.

## Public API

MimironSQL exposes an EF Core provider surface.

- Configure via `UseMimironDb2(...)` on `DbContextOptionsBuilder`.
- Query via standard EF Core LINQ (`Where`, `Select`, `Include`, etc.).
- Read-only: `SaveChanges` / `SaveChangesAsync` are not supported.
- Async query execution (`ToListAsync`, etc.) is not supported.

## Supported LINQ Operations

MimironSQL supports the following LINQ query operations:

- `Where` - Filtering
- `Select` - Projection (including anonymous types and navigation properties)
- `FirstOrDefault`, `First` - Retrieve first element
- `SingleOrDefault`, `Single` - Retrieve single element
- `Take`, `Skip` - Result limiting and pagination
- `Count`, `Any`, `All` - Aggregation
- `Include`, `ThenInclude` - Eager loading of navigation properties

**Note:** Unsupported LINQ operations will throw `NotSupportedException` at query execution time. Write operations (Insert, Update, Delete) are not supported as DB2 files are read-only.

## Navigation Configuration Examples

### One-to-One (Shared Primary Key)

```csharp
modelBuilder.Entity<Spell>()
    .HasOne(s => s.SpellName)
    .WithOne(sn => sn.Spell)
    .HasForeignKey<SpellName>(sn => sn.Id);
```

### Many-to-One (Foreign Key)

```csharp
modelBuilder.Entity<MapChallengeMode>()
    .HasOne(mc => mc.Map)
    .WithMany(m => m.MapChallengeModes)
    .HasForeignKey(mc => mc.MapID);
```

### One-to-Many (Inverse of Many-to-One)

```csharp
modelBuilder.Entity<Map>()
    .HasMany(m => m.MapChallengeModes)
    .WithOne(mc => mc.Map)
    .HasForeignKey(mc => mc.MapID);
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
public class MapConfiguration : IEntityTypeConfiguration<Map>
{
    public void Configure(EntityTypeBuilder<Map> builder)
    {
        builder.ToTable("Map");

        builder.HasMany(m => m.MapChallengeModes)
            .WithOne(mc => mc.Map)
            .HasForeignKey(mc => mc.MapID);
    }
}

// In OnModelCreating:
modelBuilder.ApplyConfigurationsFromAssembly(Assembly.GetExecutingAssembly());
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

## Performance Tips

1. **Filter by key early** - Prefer `Where(e => e.Id == id).Take(1)` over scanning
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