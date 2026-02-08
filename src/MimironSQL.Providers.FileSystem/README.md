# MimironSQL.Providers.FileSystem

Filesystem-based providers for reading DB2 files and DBD definitions from disk. Provides simple, high-performance file access for local WoW installations and cloned WoWDBDefs repositories.

## Overview

`MimironSQL.Providers.FileSystem` implements filesystem-based data access for MimironSQL:

- **DB2 Stream Provider**: Reads `.db2` files from a directory
- **DBD Provider**: Reads `.dbd` schema definitions from a directory
- **TACT Key Providers**: Simple and CSV-based TACT key management
- Efficient file discovery with case-insensitive lookup
- Minimal dependencies and straightforward configuration

## Installation

```bash
dotnet add package MimironSQL.Providers.FileSystem
```

## Package Information

- **Package ID**: `MimironSQL.Providers.FileSystem`
- **Target Framework**: .NET 10.0
- **IsPackable**: Yes
- **Dependencies**:
  - `MimironSQL.Contracts`
  - `MimironSQL.Dbd`

## Public API Reference

### DB2 Stream Provider

#### `FileSystemDb2StreamProvider`

Provides access to DB2 files from a directory.

```csharp
public sealed class FileSystemDb2StreamProvider : IDb2StreamProvider
{
    public FileSystemDb2StreamProvider(FileSystemDb2StreamProviderOptions options);
    public Stream OpenDb2Stream(string tableName);
}
```

**Configuration:**
```csharp
public sealed class FileSystemDb2StreamProviderOptions
{
    public required string Db2DirectoryPath { get; init; }
}
```

**Usage:**
```csharp
using MimironSQL.Providers;

var options = new FileSystemDb2StreamProviderOptions
{
    Db2DirectoryPath = @"C:\Program Files\World of Warcraft\DBFilesClient"
};

var provider = new FileSystemDb2StreamProvider(options);

// Open a DB2 file
using var stream = provider.OpenDb2Stream("Map");
// stream points to "C:\...\DBFilesClient\Map.db2"
```

**Features:**
- Case-insensitive table name lookup
- Efficient file discovery (scanned once on construction)
- Throws `FileNotFoundException` for missing tables
- Supports any directory structure with `.db2` files

### DBD Provider

#### `FileSystemDbdProvider`

Provides access to DBD definition files from a directory.

```csharp
public sealed class FileSystemDbdProvider : IDbdProvider
{
    public FileSystemDbdProvider(FileSystemDbdProviderOptions options);
    public IDbdFile Open(string tableName);
}
```

**Configuration:**
```csharp
public sealed class FileSystemDbdProviderOptions
{
    public required string DefinitionsDirectory { get; init; }
}
```

**Usage:**
```csharp
using MimironSQL.Providers;

var options = new FileSystemDbdProviderOptions
{
    DefinitionsDirectory = @"C:\WoWDBDefs\definitions"
};

var provider = new FileSystemDbdProvider(options);

// Open a DBD file
var dbdFile = provider.Open("Map");
// Opens "C:\WoWDBDefs\definitions\Map.dbd"
```

### TACT Key Providers

#### `SimpleTactKeyProvider`

In-memory TACT key storage with programmatic key management.

```csharp
public sealed class SimpleTactKeyProvider : ITactKeyProvider
{
    public SimpleTactKeyProvider();
    public void AddKey(ulong keyName, byte[] key);
    public bool TryGetKey(ulong keyName, out ReadOnlyMemory<byte> key);
}
```

**Usage:**
```csharp
var provider = new SimpleTactKeyProvider();

// Add keys programmatically
provider.AddKey(0x1234567890ABCDEF, new byte[] 
{ 
    0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08,
    0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E, 0x0F, 0x10
});

// Use with format
var format = new Wdc5Format(new Wdc5FileOptions
{
    TactKeyProvider = provider
});
```

#### `FileSystemTactKeyProvider`

CSV-based TACT key loading from disk.

```csharp
public sealed class FileSystemTactKeyProvider : ITactKeyProvider
{
    public FileSystemTactKeyProvider(FileSystemTactKeyProviderOptions options);
    public bool TryGetKey(ulong keyName, out ReadOnlyMemory<byte> key);
}
```

**Configuration:**
```csharp
public sealed class FileSystemTactKeyProviderOptions
{
    public required string TactKeysFilePath { get; init; }
}
```

**CSV File Format:**
```csv
KeyName,Key
FA505078126ACB3E,0102030405060708090A0B0C0D0E0F10
D1E9B5EDF9283668,1112131415161718191A1B1C1D1E1F20
```

**Usage:**
```csharp
var options = new FileSystemTactKeyProviderOptions
{
    TactKeysFilePath = @"C:\TactKeys\keys.csv"
};

var provider = new FileSystemTactKeyProvider(options);

// Keys are automatically loaded from CSV
```

## Complete Setup Example

### Basic Configuration

```csharp
using Microsoft.EntityFrameworkCore;
using MimironSQL.EntityFrameworkCore;
using MimironSQL.Providers;

// Configure all providers
var dbdProvider = new FileSystemDbdProvider(new FileSystemDbdProviderOptions
{
    DefinitionsDirectory = @"C:\WoWDBDefs\definitions"
});

var db2Provider = new FileSystemDb2StreamProvider(new FileSystemDb2StreamProviderOptions
{
    Db2DirectoryPath = @"C:\WoW\DBFilesClient"
});

var tactKeyProvider = new SimpleTactKeyProvider();
// Add keys if needed for encrypted files

// Configure EF Core
var options = new DbContextOptionsBuilder<WoWDb2Context>()
    .UseMimironDb2(db2Provider, dbdProvider, tactKeyProvider)
    .Options;

using var context = new WoWDb2Context(options);
```

### With CSV TACT Keys

```csharp
var tactKeyProvider = new FileSystemTactKeyProvider(new FileSystemTactKeyProviderOptions
{
    TactKeysFilePath = @"C:\WoW\TactKeys.csv"
});

var options = new DbContextOptionsBuilder<WoWDb2Context>()
    .UseMimironDb2(db2Provider, dbdProvider, tactKeyProvider)
    .Options;
```

### With Dependency Injection

```csharp
using Microsoft.Extensions.DependencyInjection;

var services = new ServiceCollection();

// Register providers
services.AddSingleton<IDb2StreamProvider>(sp => 
    new FileSystemDb2StreamProvider(new FileSystemDb2StreamProviderOptions
    {
        Db2DirectoryPath = @"C:\WoW\DBFilesClient"
    }));

services.AddSingleton<IDbdProvider>(sp => 
    new FileSystemDbdProvider(new FileSystemDbdProviderOptions
    {
        DefinitionsDirectory = @"C:\WoWDBDefs\definitions"
    }));

services.AddSingleton<ITactKeyProvider, SimpleTactKeyProvider>();

// Register DbContext
services.AddDbContext<WoWDb2Context>((sp, options) =>
{
    var db2Provider = sp.GetRequiredService<IDb2StreamProvider>();
    var dbdProvider = sp.GetRequiredService<IDbdProvider>();
    var tactKeyProvider = sp.GetRequiredService<ITactKeyProvider>();
    
    options.UseMimironDb2(db2Provider, dbdProvider, tactKeyProvider);
});

var serviceProvider = services.BuildServiceProvider();
var context = serviceProvider.GetRequiredService<WoWDb2Context>();
```

## Directory Structure

### DB2 Files

Expected structure for DB2 files:

```
C:\WoW\DBFilesClient\
├── Map.db2
├── Spell.db2
├── Item.db2
├── Achievement.db2
└── ... (more .db2 files)
```

The provider scans the directory for all `.db2` files on construction.

### DBD Files

Expected structure for DBD definitions:

```
C:\WoWDBDefs\definitions\
├── Map.dbd
├── Spell.dbd
├── Item.dbd
├── Achievement.dbd
└── ... (more .dbd files)
```

Clone from: [WoWDBDefs Repository](https://github.com/wowdev/WoWDBDefs)

## Error Handling

### Missing DB2 File

```csharp
try
{
    var stream = db2Provider.OpenDb2Stream("NonExistentTable");
}
catch (FileNotFoundException ex)
{
    Console.WriteLine(ex.Message);
    // "No .db2 file found for table 'NonExistentTable' in 'C:\WoW\DBFilesClient'."
}
```

### Missing DBD File

```csharp
try
{
    var dbdFile = dbdProvider.Open("NonExistentTable");
}
catch (FileNotFoundException ex)
{
    Console.WriteLine(ex.Message);
    // File not found exception with path details
}
```

### Missing TACT Key

```csharp
if (!tactKeyProvider.TryGetKey(0x1234567890ABCDEF, out var key))
{
    Console.WriteLine("TACT key not found");
    // Handle missing key (file may fail to decrypt)
}
```

## Performance Considerations

### File Discovery

The `FileSystemDb2StreamProvider` indexes all `.db2` files on construction:

```csharp
// One-time scan during construction
var provider = new FileSystemDb2StreamProvider(options);  // Scans directory

// Subsequent calls are fast (dictionary lookup)
var stream1 = provider.OpenDb2Stream("Map");      // O(1)
var stream2 = provider.OpenDb2Stream("Spell");    // O(1)
```

**Best Practice:** Create a single instance and reuse it.

### File Streams

Opened streams should be disposed promptly:

```csharp
// ✅ Good - using statement
using var stream = db2Provider.OpenDb2Stream("Map");
// Process stream...

// ✅ Good - explicit disposal
var stream = db2Provider.OpenDb2Stream("Map");
try
{
    // Process stream...
}
finally
{
    stream.Dispose();
}

// ❌ Bad - no disposal (file handle leak)
var stream = db2Provider.OpenDb2Stream("Map");
// Process stream but never dispose...
```

### TACT Key Loading

CSV keys are loaded once during provider construction:

```csharp
// One-time load
var provider = new FileSystemTactKeyProvider(options);  // Loads CSV

// Subsequent lookups are fast (dictionary lookup)
provider.TryGetKey(keyName1, out _);  // O(1)
provider.TryGetKey(keyName2, out _);  // O(1)
```

## TACT Keys CSV Format

### Format Specification

```csv
KeyName,Key
FA505078126ACB3E,0102030405060708090A0B0C0D0E0F10
D1E9B5EDF9283668,1112131415161718191A1B1C1D1E1F20
```

**Requirements:**
- Header row: `KeyName,Key`
- Key Name: 16-character hexadecimal (8 bytes / 64-bit)
- Key: 32-character hexadecimal (16 bytes / 128-bit)
- One key per line

### Generating Keys CSV

```csharp
using System.IO;
using CsvHelper;

var keys = new Dictionary<ulong, byte[]>
{
    { 0xFA505078126ACB3E, new byte[] { 0x01, 0x02, /* ... */ } },
    { 0xD1E9B5EDF9283668, new byte[] { 0x11, 0x12, /* ... */ } }
};

using var writer = new StreamWriter("keys.csv");
using var csv = new CsvWriter(writer, CultureInfo.InvariantCulture);

csv.WriteHeader<TactKeyCsvRecord>();
csv.NextRecord();

foreach (var (keyName, key) in keys)
{
    csv.WriteRecord(new TactKeyCsvRecord
    {
        KeyName = keyName.ToString("X16"),
        Key = Convert.ToHexString(key)
    });
    csv.NextRecord();
}
```

## Path Configuration Tips

### Absolute vs Relative Paths

```csharp
// ✅ Absolute path (recommended)
var options = new FileSystemDb2StreamProviderOptions
{
    Db2DirectoryPath = @"C:\WoW\DBFilesClient"
};

// ✅ Relative path (works if current directory is known)
var options = new FileSystemDb2StreamProviderOptions
{
    Db2DirectoryPath = @"..\..\data\DBFilesClient"
};

// ✅ Path.Combine for cross-platform
var basePath = Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.UserProfile), "WoW");
var options = new FileSystemDb2StreamProviderOptions
{
    Db2DirectoryPath = Path.Combine(basePath, "DBFilesClient")
};
```

### Environment Variables

```csharp
var db2Path = Environment.GetEnvironmentVariable("WOW_DB2_PATH") 
    ?? @"C:\WoW\DBFilesClient";

var options = new FileSystemDb2StreamProviderOptions
{
    Db2DirectoryPath = db2Path
};
```

### Configuration Files

```json
{
  "WoWDataPaths": {
    "Db2Directory": "C:\\WoW\\DBFilesClient",
    "DbdDirectory": "C:\\WoWDBDefs\\definitions",
    "TactKeysFile": "C:\\WoW\\TactKeys.csv"
  }
}
```

```csharp
var config = builder.Configuration.GetSection("WoWDataPaths");

services.AddSingleton<IDb2StreamProvider>(sp => 
    new FileSystemDb2StreamProvider(new FileSystemDb2StreamProviderOptions
    {
        Db2DirectoryPath = config["Db2Directory"]!
    }));
```

## Troubleshooting

### "No .db2 file found for table"

**Causes:**
1. DB2 file doesn't exist in the directory
2. Table name doesn't match file name
3. Directory path is incorrect

**Solutions:**
- Verify file exists: `Map.db2` for table name `"Map"`
- Check directory path is correct
- Ensure file has `.db2` extension

### DBD File Not Found

**Causes:**
1. DBD file doesn't exist
2. WoWDBDefs not cloned/updated
3. Directory path is incorrect

**Solutions:**
- Clone WoWDBDefs: `git clone https://github.com/wowdev/WoWDBDefs`
- Update definitions: `git pull` in WoWDBDefs directory
- Verify path points to `definitions` subdirectory

### TACT Key Not Loading

**Causes:**
1. CSV file format is incorrect
2. Key values are malformed
3. File path is wrong

**Solutions:**
- Check CSV has header: `KeyName,Key`
- Verify keys are valid hexadecimal
- Use absolute path to CSV file

## Related Packages

- **MimironSQL.Contracts**: Core interfaces
- **MimironSQL.EntityFrameworkCore**: EF Core provider
- **MimironSQL.Providers.CASC**: Alternative CASC-based providers

## See Also

- [Root README](../../README.md)
- [WoWDBDefs Repository](https://github.com/wowdev/WoWDBDefs)
- [TACT Keys](https://wowdev.wiki/TACT)
