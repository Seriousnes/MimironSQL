# MimironSQL.Dbd

DBD (Database Definition) file parser for WoWDBDefs format. Provides the schema information needed to interpret DB2 binary data.

## Overview

`MimironSQL.Dbd` parses `.dbd` files from the [WoWDBDefs repository](https://github.com/wowdev/WoWDBDefs), which contain schema definitions for World of Warcraft's DB2 tables. This package is used internally by:

- **MimironSQL.EntityFrameworkCore**: To validate entity mappings against schema
- **MimironSQL.DbContextGenerator**: To generate entity classes from definitions
- **Format Readers**: To understand column types and layouts

## Package Information

- **Package ID**: N/A (not packaged - internal use only)
- **Target Framework**: .NET Standard 2.0
- **IsPackable**: No (internal component)
- **Dependencies**:
  - `MimironSQL.Contracts`

## Role in Overall Design

```
WoWDBDefs Repository (.dbd files)
    ↓
MimironSQL.Dbd (parser)
    ↓
IDbdFile (schema model)
    ↓
┌─────────────────────────┬──────────────────────────┐
│ EF Core Provider        │ DbContextGenerator       │
│ (schema validation)     │ (entity generation)      │
└─────────────────────────┴──────────────────────────┘
```

### What DBD Provides

DBD files contain critical schema information:

1. **Column Definitions**: Name, type, array size
2. **Layout Versions**: Schema changes across WoW builds
3. **Relationships**: Foreign key references between tables
4. **Comments**: Documentation and notes
5. **Annotations**: Special column behavior (id, noninline, relation)

## Internal API

### `DbdFile`

Main parser for `.dbd` files.

```csharp
public sealed class DbdFile : IDbdFile
{
    public static DbdFile Parse(Stream stream);
    
    public IReadOnlyDictionary<string, IDbdColumn> Columns { get; }
    public IReadOnlyList<IDbdLayout> Layouts { get; }
}
```

**Usage (Internal):**
```csharp
using var stream = File.OpenRead("Map.dbd");
var dbdFile = DbdFile.Parse(stream);

// Access columns
foreach (var column in dbdFile.Columns.Values)
{
    Console.WriteLine($"{column.Name}: {column.Type}");
    if (column.ForeignTable != null)
        Console.WriteLine($"  → References {column.ForeignTable}.{column.ForeignColumn}");
}

// Access layouts
foreach (var layout in dbdFile.Layouts)
{
    Console.WriteLine($"Layout for builds: {string.Join(", ", layout.Builds)}");
}
```

### `DbdColumn`

Represents a column definition.

```csharp
public sealed class DbdColumn : IDbdColumn
{
    public string Name { get; }
    public string Type { get; }
    public bool IsArray { get; }
    public int? ArraySize { get; }
    public string? Comment { get; }
    public string? ForeignTable { get; }
    public string? ForeignColumn { get; }
}
```

**Properties:**
- `Name`: Column name (e.g., `"MapName"`, `"SpellID"`)
- `Type`: Data type (e.g., `"int"`, `"string"`, `"float"`)
- `IsArray`: True if the column is an array
- `ArraySize`: Size of array if `IsArray` is true
- `Comment`: Optional documentation comment
- `ForeignTable`: Referenced table name (if foreign key)
- `ForeignColumn`: Referenced column name (if foreign key)

### `DbdLayout`

Represents a layout for specific WoW builds.

```csharp
public sealed class DbdLayout : IDbdLayout
{
    public IReadOnlyList<IDbdBuildBlock> Builds { get; }
    public IReadOnlyList<IDbdLayoutEntry> Entries { get; }
}
```

**Purpose:**
- Maps columns to physical field positions in DB2 files
- Handles schema changes across different WoW versions
- Allows format readers to decode the correct layout for a build

### `DbdLayoutEntry`

Represents a field in a layout.

```csharp
public sealed class DbdLayoutEntry : IDbdLayoutEntry
{
    public string ColumnName { get; }
    public int? ArraySize { get; }
    public string? Annotation { get; }
}
```

**Annotations:**
- `id`: This field is the primary key
- `noninline`: String is stored inline in record, not in string table
- `relation`: Defines a foreign key relationship

### `DbdBuildBlock`

Represents a range of WoW builds using a specific layout.

```csharp
public sealed class DbdBuildBlock : IDbdBuildBlock
{
    public Version MinVersion { get; }
    public Version? MaxVersion { get; }
}
```

**Build Ranges:**
- Single version: `MinVersion = MaxVersion`
- Range: `MinVersion` to `MaxVersion` (inclusive)
- Open-ended: `MinVersion` to latest (MaxVersion = null)

## DBD File Format Example

Here's a simplified example of a `.dbd` file:

```
COLUMNS
int ID
string MapName
string Directory
int<Map::ID> ParentMapID
int InstanceType
int Flags

BUILD 1.12.1.5875-11.0.7.58162
ID
MapName
Directory
ParentMapID
InstanceType
Flags

BUILD 11.0.0.52000-11.0.7.58162
$id$ID
MapName<noninline>
Directory
ParentMapID<relation=Map::ID>
InstanceType
Flags
```

**Key Elements:**
- `COLUMNS` section: Defines all possible columns
- `BUILD` sections: Define layouts for specific version ranges
- `$id$` annotation: Marks the primary key field
- `<noninline>` annotation: String stored inline
- `<relation=...>` annotation: Foreign key reference

## How It's Used

### By EF Core Provider

The EF Core provider uses DBD to validate entity mappings:

```csharp
// Provider gets DBD file for "Map" table
var dbdFile = dbdProvider.Open("Map");

// Validates entity property "MapName" exists in schema
var column = dbdFile.Columns["MapName"];

// Checks type compatibility
if (column.Type != expectedType)
    throw new InvalidOperationException("Type mismatch");
```

### By Source Generator

The generator uses DBD to create entity classes:

```csharp
var dbdFile = DbdFile.Parse(stream);

// Generate properties from columns
foreach (var column in dbdFile.Columns.Values)
{
    if (column.Name == "ID") continue;  // ID is in base class
    
    var propertyType = MapDbdTypeToCSharp(column.Type, column.IsArray);
    EmitProperty(column.Name, propertyType);
    
    // Generate navigation property if foreign key
    if (column.ForeignTable != null)
        EmitNavigationProperty(column.ForeignTable);
}
```

### By Format Readers

Format readers use layout information to decode binary data:

```csharp
var dbdFile = dbdProvider.Open(tableName);

// Find layout for current build
var layout = dbdFile.Layouts
    .FirstOrDefault(l => l.Builds.Any(b => buildVersion >= b.MinVersion && 
                                           buildVersion <= b.MaxVersion));

// Use layout entries to decode fields in order
foreach (var entry in layout.Entries)
{
    var column = dbdFile.Columns[entry.ColumnName];
    // Decode field based on column.Type...
}
```

## Schema Validation

DBD parsing includes validation:

1. **Syntax Validation**: Ensures correct DBD format
2. **Column References**: Layout entries must reference existing columns
3. **Build Ranges**: Build blocks must be well-formed versions
4. **Relationship Validity**: Foreign key references must be valid

### Error Handling

```csharp
try
{
    var dbdFile = DbdFile.Parse(stream);
}
catch (InvalidDataException ex)
{
    // DBD file is malformed
    Console.WriteLine($"Invalid DBD: {ex.Message}");
}
catch (FormatException ex)
{
    // Version parsing failed
    Console.WriteLine($"Invalid build version: {ex.Message}");
}
```

## Type System

DBD defines the following types:

| DBD Type | Description | Example |
|----------|-------------|---------|
| `int` | Signed 32-bit integer | `123` |
| `uint` | Unsigned 32-bit integer | `456` |
| `long` | Signed 64-bit integer | `789123` |
| `ulong` | Unsigned 64-bit integer | `456789` |
| `float` | Single-precision float | `3.14` |
| `string` | UTF-8 string | `"Hello"` |
| `locstring` | Localized string | `"Hello"` (with locale variants) |
| `int<EnumName>` | Typed enum | `InstanceType` |
| `int[3]` | Array | `[1, 2, 3]` |

### Array Types

Arrays are indicated by `[size]` suffix:

```
int[3] Position      // Array of 3 ints
float[4] Quaternion  // Array of 4 floats
string[2] Names      // Array of 2 strings
```

### Foreign Keys

Foreign keys use the `<relation=...>` syntax:

```
int<Map::ID> ParentMapID<relation=Map::ID>
```

This indicates `ParentMapID` references the `ID` column in the `Map` table.

## Version Resolution

The parser resolves layouts based on WoW build version:

```csharp
// Given version 11.0.7.58162
var targetVersion = new Version(11, 0, 7, 58162);

// Find matching layout
var layout = dbdFile.Layouts.FirstOrDefault(l =>
    l.Builds.Any(b => 
        targetVersion >= b.MinVersion && 
        (b.MaxVersion == null || targetVersion <= b.MaxVersion)));
```

**Build Range Examples:**
- `1.12.1.5875`: Exact version
- `1.12.1.5875-2.0.0.6180`: Inclusive range
- `11.0.0.52000-`: Open-ended (11.0.0+ to latest)

## Performance Considerations

### Parsing Performance

- DBD files are small (typically 1-10 KB)
- Parsing is fast (~1ms per file)
- Results should be cached to avoid repeated parsing

### Caching Strategy

```csharp
// Cache parsed DBD files
private readonly Dictionary<string, IDbdFile> _cache = new();

public IDbdFile GetDbdFile(string tableName)
{
    if (!_cache.TryGetValue(tableName, out var dbdFile))
    {
        using var stream = OpenDbdStream(tableName);
        dbdFile = DbdFile.Parse(stream);
        _cache[tableName] = dbdFile;
    }
    return dbdFile;
}
```

## Integration with Providers

The `MimironSQL.Dbd` parser is used via the `IDbdProvider` interface:

```csharp
public interface IDbdProvider
{
    IDbdFile Open(string tableName);
}
```

Providers implement this interface to supply DBD files:

- **FileSystemDbdProvider**: Reads from disk
- **CascDbdProvider**: Downloads from GitHub (planned)
- **CachedDbdProvider**: Adds caching layer (planned)

## Testing

DBD parsing is tested against:

1. **Real WoWDBDefs**: Actual `.dbd` files from the repository
2. **Edge Cases**: Complex layouts, large arrays, multiple builds
3. **Malformed Input**: Invalid syntax, bad version strings
4. **Performance**: Parsing speed benchmarks

## Related Packages

- **MimironSQL.Contracts**: Interface definitions (`IDbdFile`, etc.)
- **MimironSQL.EntityFrameworkCore**: Uses DBD for schema validation
- **MimironSQL.DbContextGenerator**: Uses DBD for entity generation
- **MimironSQL.Providers.FileSystem**: Provides filesystem-based DBD access

## See Also

- [Root README](../../README.md)
- [WoWDBDefs Repository](https://github.com/wowdev/WoWDBDefs)
- [DBD Format Documentation](https://wowdev.wiki/DBD)
- [Architecture Overview](../../.github/instructions/architecture.md)
