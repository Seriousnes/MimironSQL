# MimironSQL.Formats.Wdc5

WDC5 format reader and parser for World of Warcraft DB2 files. Provides high-performance binary parsing with support for encrypted sections, sparse data, and relationship metadata.

## Overview

`MimironSQL.Formats.Wdc5` implements the WDC5 (World of Warcraft Database Client version 5) format, which is used in modern World of Warcraft expansions. This package provides:

- Binary parsing of WDC5 file headers and data
- Support for encrypted sections via Salsa20 (requires TACT keys)
- Sparse data handling (pallet and common data)
- String table parsing (inline and external)
- Relationship metadata extraction
- Efficient row access via `RowHandle`

## Installation

```bash
dotnet add package MimironSQL.Formats.Wdc5
```

## Package Information

- **Package ID**: `MimironSQL.Formats.Wdc5`
- **Target Framework**: .NET 10.0
- **Dependencies**:
  - `MimironSQL.Contracts`
  - `Salsa20` (for encrypted sections)

## WDC5 Format Overview

WDC5 is a binary format used by World of Warcraft to store database tables. Key features:

- **Header**: Contains metadata about the file structure
- **Record Data**: Fixed-size or variable-size records
- **String Table**: Stores string data referenced by records
- **Encrypted Sections**: Some files have encrypted data requiring TACT keys
- **Sparse Data**: Pallet and common data for efficient storage
- **Relationships**: Foreign key metadata embedded in the file

## Public API

### `Wdc5Format`

Main entry point for opening WDC5 files.

```csharp
public sealed class Wdc5Format : IDb2Format
{
    public Wdc5Format(Wdc5FileOptions? options = null);
    
    public Db2Format Format { get; }
    public IDb2File OpenFile(Stream stream);
    public Db2FileLayout GetLayout(IDb2File file);
}
```

**Usage:**
```csharp
using MimironSQL.Formats;

var format = new Wdc5Format(new Wdc5FileOptions
{
    TactKeyProvider = myTactKeyProvider  // Optional, for encrypted files
});

using var stream = File.OpenRead("Map.db2");
using var file = format.OpenFile(stream);

Console.WriteLine($"Records: {file.RecordCount}");
```

### `Wdc5FileOptions`

Configuration options for opening WDC5 files.

```csharp
public sealed class Wdc5FileOptions
{
    public ITactKeyProvider? TactKeyProvider { get; init; }
}
```

**Properties:**
- `TactKeyProvider`: Provides TACT encryption keys for encrypted sections (optional)

### `Wdc5File`

Represents an open WDC5 file with row access.

```csharp
public sealed class Wdc5File : IDb2File<RowHandle>
{
    public Type RowType { get; }
    public int RecordCount { get; }
    public RowHandle GetRow(int recordIndex);
}
```

**Example:**
```csharp
using var file = format.OpenFile(stream);

for (int i = 0; i < file.RecordCount; i++)
{
    var row = file.GetRow(i);
    var id = row.Id;
    var recordData = row.RecordData;
    // Parse recordData based on schema...
}
```

### `RowHandle`

Efficient read-only handle to a row's binary data.

```csharp
public readonly struct RowHandle
{
    public ReadOnlyMemory<byte> RecordData { get; }
    public int RecordSize { get; }
    public int Id { get; }
}
```

**Key Points:**
- Zero allocation - just a handle to the underlying buffer
- `RecordData` contains the raw binary record
- `RecordSize` is the size of the record in bytes
- `Id` is the primary key of the record

## Usage Examples

### Basic File Reading

```csharp
using MimironSQL.Formats;

var format = new Wdc5Format();

using var stream = File.OpenRead(@"C:\WoW\DBFilesClient\Map.db2");
using var file = format.OpenFile(stream);

Console.WriteLine($"File has {file.RecordCount} records");

// Access first record
var firstRow = file.GetRow(0);
Console.WriteLine($"First record ID: {firstRow.Id}");
Console.WriteLine($"Record size: {firstRow.RecordSize} bytes");
```

### Reading with Encryption Support

For files with encrypted sections, provide a TACT key provider:

```csharp
var tactKeyProvider = new SimpleTactKeyProvider();

// Add known TACT keys
tactKeyProvider.AddKey(0x1234567890ABCDEF, keyBytes);

var format = new Wdc5Format(new Wdc5FileOptions
{
    TactKeyProvider = tactKeyProvider
});

using var stream = File.OpenRead("EncryptedFile.db2");
using var file = format.OpenFile(stream);

// File is automatically decrypted if TACT keys are available
```

### Getting Layout Information

```csharp
var format = new Wdc5Format();
using var file = format.OpenFile(stream);

var layout = format.GetLayout(file);

Console.WriteLine($"Record Count: {layout.RecordCount}");
Console.WriteLine($"Field Count: {layout.FieldCount}");
Console.WriteLine($"Record Size: {layout.RecordSize}");
Console.WriteLine($"String Table Size: {layout.StringTableSize}");
```

### Integration with EF Core Provider

The format is automatically used by the EF Core provider:

```csharp
using Microsoft.EntityFrameworkCore;
using MimironSQL.EntityFrameworkCore;

services.AddSingleton<IDb2Format>(sp =>
{
    var tactKeyProvider = sp.GetRequiredService<ITactKeyProvider>();
    return new Wdc5Format(new Wdc5FileOptions
    {
        TactKeyProvider = tactKeyProvider
    });
});

// The EF provider will use Wdc5Format to open DB2 files
```

## File Structure Details

### Header Layout

The WDC5 header contains:
- Magic signature ('WDC5')
- Record count
- Field count
- Record size
- String table size
- Table hash
- Layout hash
- Flags (sparse data, encrypted, etc.)
- ID field metadata
- Relationship metadata

### Record Storage

Records can be stored in two ways:

1. **Normal Storage**: Fixed-size records in a contiguous block
2. **Sparse Storage**: Variable-size records with pallet/common data

### String Tables

Strings are stored in a dedicated string table:
- Referenced by offset from record data
- Can be inline (within the file) or external
- UTF-8 encoded, null-terminated

### Encrypted Sections

Some WDC5 files have encrypted data sections:
- Encryption: Salsa20 stream cipher
- Requires TACT keys to decrypt
- Key lookup by key name (uint64)
- Decryption happens automatically when keys are available

## Performance Characteristics

- **Efficient row access**: `RowHandle` provides direct memory access
- **Lazy string loading**: Strings are only decoded when accessed
- **Efficient binary parsing**: Uses `ReadOnlySpan<byte>` for parsing
- **Minimal memory overhead**: File data is memory-mapped when possible

## Implementation Details

### Row Handle Design

`RowHandle` is a lightweight struct that provides access to raw binary data without copying:

```csharp
var row = file.GetRow(index);
var data = row.RecordData.Span;  // Direct access to binary data

// Read an int32 at offset 0
int field1 = BitConverter.ToInt32(data.Slice(0, 4));

// Read an int32 at offset 4
int field2 = BitConverter.ToInt32(data.Slice(4, 4));
```

### Sparse Data Handling

Sparse data is automatically handled:
- **Pallet Data**: Small set of unique values indexed by records
- **Common Data**: Single value shared across multiple records
- Transparently resolved during row access

### Encryption Flow

1. File is opened, header is parsed
2. If encrypted flag is set, attempt to get TACT key
3. If key is available, decrypt the section using Salsa20
4. Continue parsing decrypted data
5. If key is unavailable, throw exception

## Format Support

### Supported Features

- ✅ Normal and sparse record storage
- ✅ Inline and external string tables
- ✅ Encrypted sections (with TACT keys)
- ✅ ID block parsing
- ✅ Relationship metadata
- ✅ Pallet and common data
- ✅ Copy table data
- ✅ Variable-size records

### Version Compatibility

WDC5 format was introduced in:
- World of Warcraft: Legion (7.3.0+)
- World of Warcraft: Battle for Azeroth (8.x)
- World of Warcraft: Shadowlands (9.x)
- World of Warcraft: Dragonflight (10.x)
- World of Warcraft: The War Within (11.x)

## Troubleshooting

### "Invalid WDC5 magic"

The file is not a valid WDC5 file. Check:
- File is actually a `.db2` file
- File is not corrupted
- File uses WDC5 format (not WDC4, WDC6, etc.)

### "TACT key not found"

An encrypted section requires a TACT key that wasn't provided. Solutions:
- Provide the required TACT key via `ITactKeyProvider`
- Obtain TACT keys from community resources
- Some files cannot be decrypted without official keys

### "String offset out of bounds"

A string reference points outside the string table. This typically indicates:
- File corruption
- Incorrect parsing of record data
- Schema mismatch (wrong field interpretation)

## Advanced Usage

### Custom Row Handling

For advanced scenarios, you can create custom row types:

```csharp
public readonly struct CustomRow
{
    public int Id { get; init; }
    public ReadOnlyMemory<byte> Data { get; init; }
    public IStringTable StringTable { get; init; }
}

// Implement IDb2File<CustomRow> for custom handling
```

### Direct Binary Parsing

```csharp
var row = file.GetRow(index);
var span = row.RecordData.Span;

// Parse fields directly based on schema
int field1 = BinaryPrimitives.ReadInt32LittleEndian(span.Slice(0));
int field2 = BinaryPrimitives.ReadInt32LittleEndian(span.Slice(4));
float field3 = BinaryPrimitives.ReadSingleLittleEndian(span.Slice(8));
```

## Related Packages

- **MimironSQL.Contracts**: Core abstractions
- **Salsa20**: Encryption support
- **MimironSQL.EntityFrameworkCore**: EF Core integration
- **MimironSQL.Providers.FileSystem**: File access providers

## See Also

- [Root README](../../README.md)
- [DB2 Format Specification](https://wowdev.wiki/DB2)
- [WDC5 Format Details](https://wowdev.wiki/DB2#WDC5)
- [TACT Keys](https://wowdev.wiki/TACT)
