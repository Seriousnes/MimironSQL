# MimironSQL.Contracts

Core interfaces and types for extending MimironSQL with custom formats and providers.

## Overview

This package defines the public contracts that enable:
- **Custom DB2 format readers** (e.g., WDC4, WDC6, future formats)
- **Custom data source providers** (e.g., network storage, cloud, archives)
- **Custom schema providers** (alternatives to WoWDBDefs)
- **Custom encryption key providers** (TACT key management)

## Extension Interfaces

### IDb2Format - Custom Format Readers

Implement to support a new DB2 format version.

```csharp
public interface IDb2Format
{
    Db2Format Format { get; }           // Format identifier (e.g., Wdc5)
    IDb2File OpenFile(Stream stream);   // Parse binary stream
    Db2FileLayout GetLayout(IDb2File file); // Extract metadata
}
```

**Implementation requirements:**
- Parse format-specific headers
- Return `IDb2File<TRow>` implementation
- Support multiple concurrent files

**Example:**
```csharp
public class Wdc6Format : IDb2Format
{
    public Db2Format Format => Db2Format.Wdc6;
    
    public IDb2File OpenFile(Stream stream)
    {
        return new Wdc6File(stream); // Your parser
    }
    
    public Db2FileLayout GetLayout(IDb2File file)
    {
        var wdc6 = (Wdc6File)file;
        return new Db2FileLayout
        {
            RecordCount = wdc6.RecordCount,
            FieldCount = wdc6.FieldCount,
            RecordSize = wdc6.RecordSize
        };
    }
}
```

### IDb2File / IDb2File&lt;TRow&gt; - File Access

Represents an open DB2 file with row access.

```csharp
public interface IDb2File : IDisposable
{
    Type RowType { get; }              // Type of row handle
    int RecordCount { get; }           // Number of records
    object GetRow(int recordIndex);    // Get row by index
}

public interface IDb2File<TRow> : IDb2File
{
    new TRow GetRow(int recordIndex);  // Strongly-typed access
}
```

**Implementation requirements:**
- Efficient random access to rows
- Thread-safe if concurrent access needed
- Dispose underlying resources

### IDb2StreamProvider - Data Source Access

Provide DB2 file streams from any source.

```csharp
public interface IDb2StreamProvider
{
    Stream OpenDb2Stream(string tableName);
}
```

**Implementation requirements:**
- Map table name to DB2 file
- Return readable stream
- Throw `FileNotFoundException` if not found

**Example:**
```csharp
public class HttpDb2StreamProvider : IDb2StreamProvider
{
    private readonly HttpClient _client;
    private readonly string _baseUrl;
    
    public Stream OpenDb2Stream(string tableName)
    {
        var url = $"{_baseUrl}/{tableName}.db2";
        var response = _client.GetAsync(url).Result;
        return response.Content.ReadAsStream();
    }
}
```

### IDbdProvider - Schema Access

Provide DBD schema definition files.

```csharp
public interface IDbdProvider
{
    IDbdFile Open(string tableName);
}
```

**Implementation requirements:**
- Return parsed DBD file
- Support concurrent access
- Throw `FileNotFoundException` if not found

### ITactKeyProvider - Encryption Keys

Provide TACT encryption keys for decrypting DB2 sections.

```csharp
public interface ITactKeyProvider
{
    bool TryGetKey(ulong keyName, out ReadOnlyMemory<byte> key);
}
```

**Implementation requirements:**
- Return 16-byte keys when available
- Thread-safe lookup
- Return `false` for unknown keys

**Example:**
```csharp
public class DatabaseTactKeyProvider : ITactKeyProvider
{
    private readonly Dictionary<ulong, byte[]> _keys = new();
    
    public void LoadFromDatabase()
    {
        // Load keys from database
    }
    
    public bool TryGetKey(ulong keyName, out ReadOnlyMemory<byte> key)
    {
        if (_keys.TryGetValue(keyName, out var bytes))
        {
            key = bytes;
            return true;
        }
        key = default;
        return false;
    }
}
```

## Key Types

### RowHandle

Efficient read-only access to binary record data.

```csharp
public readonly struct RowHandle
{
    public ReadOnlyMemory<byte> RecordData { get; }
    public int RecordSize { get; }
    public int Id { get; }
}
```

### Db2FileLayout

Metadata about DB2 file structure.

```csharp
public sealed class Db2FileLayout
{
    public int RecordCount { get; init; }
    public int FieldCount { get; init; }
    public int RecordSize { get; init; }
    public int StringTableSize { get; init; }
}
```

### DBD Interfaces

For implementing custom schema sources:
- `IDbdFile` - Parsed DBD file
- `IDbdColumn` - Column definition
- `IDbdLayout` - Build-specific layout
- `IDbdLayoutEntry` - Field entry

## Registration with EF Core

```csharp
services.AddSingleton<IDb2Format, MyCustomFormat>();
services.AddSingleton<IDb2StreamProvider, MyCustomProvider>();
```

Target: .NET Standard 2.0
