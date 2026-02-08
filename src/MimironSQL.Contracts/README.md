# MimironSQL.Contracts

Public contracts and abstractions for extending MimironSQL with custom formats, providers, and DBD implementations.

## Overview

`MimironSQL.Contracts` is the core abstraction layer that defines the interfaces and base types used throughout the MimironSQL ecosystem. This package is designed for extensibility, allowing developers to:

- Implement custom DB2 format readers (e.g., WDC4, WDC6, future formats)
- Create custom data source providers (e.g., network storage, cloud storage, compressed archives)
- Build alternative DBD (schema definition) providers
- Extend TACT key management for encrypted DB2 sections

## Installation

```bash
dotnet add package MimironSQL.Contracts
```

## Package Information

- **Package ID**: `MimironSQL.Contracts`
- **Target Framework**: .NET Standard 2.0
- **Dependencies**: `System.Buffers`, `System.Memory`

## Core Abstractions

### Format Abstractions

The format abstractions define how DB2 files are parsed and read.

#### `IDb2Format`

The main interface for implementing a DB2 format reader.

```csharp
public interface IDb2Format
{
    Db2Format Format { get; }
    IDb2File OpenFile(Stream stream);
    Db2FileLayout GetLayout(IDb2File file);
}
```

**Example Implementation:**
```csharp
public class Wdc5Format : IDb2Format
{
    public Db2Format Format => Db2Format.Wdc5;
    
    public IDb2File OpenFile(Stream stream)
    {
        return new Wdc5File(stream, options);
    }
    
    public Db2FileLayout GetLayout(IDb2File file)
    {
        // Return layout information from file headers
    }
}
```

#### `IDb2File` and `IDb2File<TRow>`

Represents an open DB2 file with access to its rows.

```csharp
public interface IDb2File : IDisposable
{
    Type RowType { get; }
    int RecordCount { get; }
    object GetRow(int recordIndex);
}

public interface IDb2File<TRow> : IDb2File
{
    new TRow GetRow(int recordIndex);
}
```

**Key Points:**
- `IDb2File<TRow>` is strongly typed for specific row handle types
- Row handles (like `RowHandle`) provide efficient access to raw binary data
- `RecordCount` returns the total number of records in the file

#### `Db2FileLayout`

Describes the physical layout of a DB2 file.

```csharp
public sealed class Db2FileLayout
{
    public int RecordCount { get; init; }
    public int FieldCount { get; init; }
    public int RecordSize { get; init; }
    public int StringTableSize { get; init; }
}
```

#### `RowHandle`

An efficient read-only handle to a row's binary data. Used by format implementations to provide efficient access to record data.

```csharp
public readonly struct RowHandle(ReadOnlyMemory<byte> recordData, int recordSize, int id)
{
    public ReadOnlyMemory<byte> RecordData { get; } = recordData;
    public int RecordSize { get; } = recordSize;
    public int Id { get; } = id;
}
```

### Provider Abstractions

Providers supply streams for DB2 files and DBD definitions.

#### `IDb2StreamProvider`

Provides access to DB2 file streams from any data source.

```csharp
public interface IDb2StreamProvider
{
    Stream OpenDb2Stream(string tableName);
}
```

**Example Implementation:**
```csharp
public class FileSystemDb2StreamProvider : IDb2StreamProvider
{
    private readonly string _db2Directory;
    
    public Stream OpenDb2Stream(string tableName)
    {
        var path = Path.Combine(_db2Directory, $"{tableName}.db2");
        return File.OpenRead(path);
    }
}
```

**Use Cases:**
- File system access (local or network drives)
- CASC storage containers
- HTTP/cloud storage
- Compressed archives
- Cached/buffered access

#### `IDbdProvider`

Provides access to DBD (schema definition) files.

```csharp
public interface IDbdProvider
{
    IDbdFile Open(string tableName);
}
```

**Example Implementation:**
```csharp
public class FileSystemDbdProvider : IDbdProvider
{
    private readonly string _definitionsDirectory;
    
    public IDbdFile Open(string tableName)
    {
        var path = Path.Combine(_definitionsDirectory, $"{tableName}.dbd");
        return DbdFile.Parse(File.OpenRead(path));
    }
}
```

#### `ITactKeyProvider`

Provides TACT encryption keys for encrypted DB2 sections.

```csharp
public interface ITactKeyProvider
{
    bool TryGetKey(ulong keyName, out ReadOnlyMemory<byte> key);
}
```

**Example Implementation:**
```csharp
public class SimpleTactKeyProvider : ITactKeyProvider
{
    private readonly Dictionary<ulong, byte[]> _keys = new();
    
    public void AddKey(ulong keyName, byte[] key)
    {
        _keys[keyName] = key;
    }
    
    public bool TryGetKey(ulong keyName, out ReadOnlyMemory<byte> key)
    {
        if (_keys.TryGetValue(keyName, out var keyBytes))
        {
            key = keyBytes;
            return true;
        }
        key = default;
        return false;
    }
}
```

### DBD (Schema Definition) Abstractions

These interfaces define the structure of DBD files (WoWDBDefs format).

#### `IDbdFile`

Represents a parsed DBD file containing schema information.

```csharp
public interface IDbdFile
{
    IReadOnlyDictionary<string, IDbdColumn> Columns { get; }
    IReadOnlyList<IDbdLayout> Layouts { get; }
}
```

#### `IDbdColumn`

Describes a column in a DB2 table.

```csharp
public interface IDbdColumn
{
    string Name { get; }
    string Type { get; }
    bool IsArray { get; }
    int? ArraySize { get; }
    string? Comment { get; }
    string? ForeignTable { get; }
    string? ForeignColumn { get; }
}
```

#### `IDbdLayout`

Describes a specific build's layout of a DB2 table.

```csharp
public interface IDbdLayout
{
    IReadOnlyList<IDbdBuildBlock> Builds { get; }
    IReadOnlyList<IDbdLayoutEntry> Entries { get; }
}
```

#### `IDbdLayoutEntry`

Represents a field entry in a layout.

```csharp
public interface IDbdLayoutEntry
{
    string ColumnName { get; }
    int? ArraySize { get; }
    string? Annotation { get; }
}
```

## Attributes

### `OverridesSchemaAttribute`

Marks entity properties that override the default schema mapping from DBD definitions.

```csharp
[AttributeUsage(AttributeTargets.Property, AllowMultiple = false)]
public sealed class OverridesSchemaAttribute : Attribute
{
}
```

**Usage:**
```csharp
public class CustomEntity
{
    public int Id { get; set; }
    
    [OverridesSchema]
    public string? CustomFieldName { get; set; }  // Won't be validated against DBD schema
}
```

## Extension Points

### Creating a Custom Format

To support a new DB2 format (e.g., WDC6):

1. Implement `IDb2Format`:
   ```csharp
   public class Wdc6Format : IDb2Format
   {
       public Db2Format Format => Db2Format.Wdc6;
       public IDb2File OpenFile(Stream stream) { /* ... */ }
       public Db2FileLayout GetLayout(IDb2File file) { /* ... */ }
   }
   ```

2. Implement `IDb2File<TRow>`:
   ```csharp
   public class Wdc6File : IDb2File<RowHandle>
   {
       public Type RowType => typeof(RowHandle);
       public int RecordCount { get; }
       public RowHandle GetRow(int recordIndex) { /* ... */ }
   }
   ```

3. Register your format with the EF provider.

### Creating a Custom Provider

To support a new data source:

```csharp
public class HttpDb2StreamProvider : IDb2StreamProvider
{
    private readonly HttpClient _httpClient;
    private readonly string _baseUrl;
    
    public Stream OpenDb2Stream(string tableName)
    {
        var url = $"{_baseUrl}/{tableName}.db2";
        var response = _httpClient.GetAsync(url).Result;
        return response.Content.ReadAsStream();
    }
}
```

## Best Practices

1. **Dispose Streams**: Always dispose `IDb2File` instances to release file handles
2. **Thread Safety**: Format and provider implementations should be thread-safe for concurrent queries
3. **Performance**: Use `ReadOnlyMemory<byte>` and `ReadOnlySpan<byte>` for efficient parsing
4. **Error Handling**: Throw descriptive exceptions for invalid formats or missing files

## Related Packages

- **MimironSQL.Formats.Wdc5**: WDC5 format implementation
- **MimironSQL.Providers.FileSystem**: Filesystem-based providers
- **MimironSQL.Providers.CASC**: CASC storage providers
- **MimironSQL.EntityFrameworkCore**: EF Core database provider

## See Also

- [Architecture Overview](../../.github/instructions/architecture.md)
- [WoWDBDefs Repository](https://github.com/wowdev/WoWDBDefs)
- [DB2 Format Specification](https://wowdev.wiki/DB2)
