# MimironSQL.Providers.CASC

CASC (Content Addressable Storage Container) provider for reading DB2 files directly from World of Warcraft game archives. Query DB2 data without extracting files to disk.

## Overview

`MimironSQL.Providers.CASC` provides direct access to DB2 files stored in Blizzard's CASC storage format. This is the native archive format used by modern World of Warcraft clients.

**Key Benefits:**
- **No Extraction Required**: Read DB2 files directly from WoW installation
- **Always Up-to-Date**: Automatically uses current game files after patches
- **Space Efficient**: No need to maintain extracted copies
- **Version Accurate**: Guaranteed to match installed game version

## Package Information

- **Package ID**: `MimironSQL.Providers.CASC`
- **Target Framework**: .NET 10.0
- **IsPackable**: No (currently bundled with EF Core provider)
- **Dependencies**:
  - `K4os.Compression.LZ4` (for decompression)
  - `CsvHelper` (for parsing manifest CSV)
  - `Microsoft.Extensions.Configuration.Abstractions`
  - `Microsoft.Extensions.DependencyInjection.Abstractions`
  - `Microsoft.Extensions.Http`
  - `Microsoft.Extensions.Options`
  - `MimironSQL.Contracts`

## Installation

The CASC provider is currently included with the EF Core package:

```bash
dotnet add package MimironSQL.EntityFrameworkCore
```

## Quick Start

### Basic Configuration

```csharp
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Options;
using MimironSQL.EntityFrameworkCore;
using MimironSQL.Providers;

// 1. Configure DBD provider
var testDataDir = @"C:\WoWDBDefs\definitions";
var dbdProvider = new FileSystemDbdProvider(
    new FileSystemDbdProviderOptions(testDataDir));

// 2. Configure manifest provider (maps table names to FileDataIDs)
var manifestOptions = new WowDb2ManifestOptions
{
    CacheDirectory = testDataDir,
    AssetName = "manifest.json",
};

using var httpClient = new HttpClient();
var wowDb2ManifestProvider = new WowDb2ManifestProvider(
    httpClient, Options.Create(manifestOptions));

var manifestProvider = new LocalFirstManifestProvider(
    wowDb2ManifestProvider, Options.Create(manifestOptions));

// Download/cache manifest if not already present
await manifestProvider.EnsureManifestExistsAsync();

// 3. Open CASC storage from WoW installation
var wowInstallRoot = @"C:\Program Files\World of Warcraft";
var storage = await CascStorage.OpenInstallRootAsync(wowInstallRoot);

// 4. Create CASC DB2 provider
var db2Provider = new CascDBCProvider(storage, manifestProvider);

// 5. Configure TACT key provider
var tactKeyProvider = new SimpleTactKeyProvider();

// 6. Configure EF Core DbContext
var options = new DbContextOptionsBuilder<WoWDb2Context>()
    .UseMimironDb2(db2Provider, dbdProvider, tactKeyProvider)
    .Options;

using var context = new WoWDb2Context(options);

// 7. Query as normal!
var maps = context.Map.Take(10).ToList();
```

## Public API Reference

### `CascStorage`

Opens and reads from CASC archives.

```csharp
public sealed class CascStorage
{
    public static Task<CascStorage> OpenInstallRootAsync(
        string installRoot, 
        CancellationToken cancellationToken = default);
    
    public Task<Stream> OpenDb2ByFileDataIdAsync(
        int fileDataId, 
        CancellationToken cancellationToken = default);
    
    public Task<Stream?> TryOpenDb2ByFileDataIdAsync(
        int fileDataId, 
        CancellationToken cancellationToken = default);
    
    public event Action<CascStorageEncryptedBlteBlocksSkipped>? EncryptedBlteBlocksSkipped;
}
```

**Usage:**
```csharp
// Open CASC from WoW installation
var storage = await CascStorage.OpenInstallRootAsync(@"C:\WoW");

// Open DB2 by FileDataID
var stream = await storage.OpenDb2ByFileDataIdAsync(123456);

// Try open (returns null if not found)
var maybeStream = await storage.TryOpenDb2ByFileDataIdAsync(789012);
```

### `CascDBCProvider`

Implements `IDb2StreamProvider` for CASC archives.

```csharp
public sealed class CascDBCProvider : IDb2StreamProvider
{
    public CascDBCProvider(
        CascStorage storage, 
        IManifestProvider manifestProvider);
    
    public Stream OpenDb2Stream(string tableName);
}
```

**Usage:**
```csharp
var storage = await CascStorage.OpenInstallRootAsync(wowPath);
var db2Provider = new CascDBCProvider(storage, manifestProvider);

// Opens "Map.db2" from CASC
using var stream = db2Provider.OpenDb2Stream("Map");
```

### `WowDb2ManifestProvider`

Downloads and caches the DB2 manifest from WoWDBDefs GitHub.

```csharp
public sealed class WowDb2ManifestProvider : IWowDb2ManifestProvider, IManifestProvider
{
    public WowDb2ManifestProvider(
        HttpClient httpClient, 
        IOptions<WowDb2ManifestOptions> options);
    
    public Task EnsureManifestExistsAsync(
        CancellationToken cancellationToken = default);
    
    public Task<int?> TryResolveDb2FileDataIdAsync(
        string db2NameOrPath, 
        CancellationToken cancellationToken = default);
}
```

**Configuration:**
```csharp
var options = new WowDb2ManifestOptions
{
    Owner = "wowdev",                    // GitHub owner (default)
    Repository = "WoWDBDefs",            // GitHub repo (default)
    AssetName = "manifest.json",         // Asset name (default)
    CacheDirectory = @"C:\cache\path",   // Optional cache location
    HttpTimeoutSeconds = 60              // Download timeout (default)
};
```

### `LocalFirstManifestProvider`

Checks local cache before downloading manifest.

```csharp
public sealed class LocalFirstManifestProvider : IManifestProvider
{
    public LocalFirstManifestProvider(
        IManifestProvider fallback, 
        IOptions<WowDb2ManifestOptions> options);
    
    public Task EnsureManifestExistsAsync(
        CancellationToken cancellationToken = default);
    
    public Task<int?> TryResolveDb2FileDataIdAsync(
        string db2NameOrPath, 
        CancellationToken cancellationToken = default);
}
```

**Usage:**
```csharp
// Wraps WowDb2ManifestProvider with local caching
var localFirstProvider = new LocalFirstManifestProvider(
    wowDb2ManifestProvider, 
    Options.Create(manifestOptions));

// First call checks cache, falls back to download if needed
await localFirstProvider.EnsureManifestExistsAsync();

// Subsequent calls use cached manifest
var fileDataId = await localFirstProvider.TryResolveDb2FileDataIdAsync("Map");
```

## CASC Architecture

### How CASC Works

CASC is Blizzard's Content Addressable Storage Container format:

1. **Content Addressing**: Files are stored by content hash, not by path
2. **Encoding Layer**: Content keys (CKeys) map to encoding keys (EKeys)
3. **Archive Storage**: EKeys point to physical locations in `.idx` and data files
4. **Compression**: Data is compressed with LZ4 and other algorithms
5. **Encryption**: Some blocks are encrypted (skipped if no key available)

### Data Flow

```
Table Name ("Map")
    ↓
Manifest Provider (table → FileDataID)
    ↓
CASC Root Index (FileDataID → ContentKey)
    ↓
CASC Encoding Index (ContentKey → EncodingKey)
    ↓
CASC Archive Reader (EncodingKey → BLTE block)
    ↓
BLTE Decoder (decompress + decrypt)
    ↓
DB2 Stream
```

### Components

#### Build Configuration

- **BuildInfo**: Maps product to build configuration
- **BuildConfig**: Contains root and encoding keys
- **InstallLayout**: Detects WoW installation structure

#### Indices

- **Root Index**: Maps FileDataIDs to content keys
- **Encoding Index**: Maps content keys to encoding keys
- **Archive Index**: Maps encoding keys to physical locations

#### Decompression

- **BLTE Decoder**: Handles Block-based compression/encryption
- **LZ4 Support**: Decompresses LZ4-compressed blocks
- **Encrypted Blocks**: Skips encrypted blocks (emits event)

## Complete Example

Here's a complete working example:

```csharp
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Options;
using MimironSQL.EntityFrameworkCore;
using MimironSQL.Providers;

public class Program
{
    public static async Task Main()
    {
        // 1. Configure paths
        var wowInstallRoot = @"C:\Program Files\World of Warcraft";
        var dbdDirectory = @"C:\WoWDBDefs\definitions";
        var cacheDirectory = dbdDirectory; // Reuse DBD dir for manifest cache
        
        // 2. Setup manifest provider
        var manifestOptions = new WowDb2ManifestOptions
        {
            CacheDirectory = cacheDirectory,
            AssetName = "manifest.json"
        };
        
        using var httpClient = new HttpClient();
        var wowDb2Manifest = new WowDb2ManifestProvider(
            httpClient, Options.Create(manifestOptions));
        
        var manifestProvider = new LocalFirstManifestProvider(
            wowDb2Manifest, Options.Create(manifestOptions));
        
        // Download manifest if not cached
        await manifestProvider.EnsureManifestExistsAsync();
        
        // 3. Open CASC storage
        Console.WriteLine("Opening CASC storage...");
        var storage = await CascStorage.OpenInstallRootAsync(wowInstallRoot);
        
        // 4. Create providers
        var db2Provider = new CascDBCProvider(storage, manifestProvider);
        var dbdProvider = new FileSystemDbdProvider(
            new FileSystemDbdProviderOptions(dbdDirectory));
        var tactKeyProvider = new SimpleTactKeyProvider();
        
        // 5. Configure EF Core
        var options = new DbContextOptionsBuilder<WoWDb2Context>()
            .UseMimironDb2(db2Provider, dbdProvider, tactKeyProvider)
            .Options;
        
        // 6. Query!
        using var context = new WoWDb2Context(options);
        
        Console.WriteLine("Querying maps from CASC...");
        var maps = context.Map
            .Where(m => m.Directory != null && m.Directory.Contains("dungeon"))
            .Take(5)
            .ToList();
        
        foreach (var map in maps)
        {
            Console.WriteLine($"  {map.Id}: {map.MapName} ({map.Directory})");
        }
    }
}
```

## Configuration Options

### `WowDb2ManifestOptions`

```csharp
public sealed record WowDb2ManifestOptions
{
    // GitHub owner (default: "wowdev")
    public string Owner { get; init; } = "wowdev";
    
    // GitHub repository (default: "WoWDBDefs")
    public string Repository { get; init; } = "WoWDBDefs";
    
    // Manifest asset name (default: "manifest.json")
    public string AssetName { get; init; } = "manifest.json";
    
    // Cache directory (default: %LOCALAPPDATA%\CASC.Net\wowdbdefs)
    public string? CacheDirectory { get; init; }
    
    // HTTP timeout in seconds (default: 60)
    public int HttpTimeoutSeconds { get; init; } = 60;
}
```

### Default Cache Location

If `CacheDirectory` is not specified:
- Windows: `%LOCALAPPDATA%\CASC.Net\wowdbdefs`
- Linux/Mac: `~/.local/share/CASC.Net/wowdbdefs`

## Manifest Format

The manifest maps table names to FileDataIDs:

```json
{
  "Map": 1349477,
  "Spell": 1375579,
  "Item": 801573,
  ...
}
```

Or as an array:

```json
[
  { "tableName": "Map", "db2FileDataID": 1349477 },
  { "tableName": "Spell", "db2FileDataID": 1375579 }
]
```

## Encrypted Blocks

CASC may contain encrypted BLTE blocks. When encryption keys are not available:

- Encrypted blocks are **skipped** (not decrypted)
- An event is emitted: `EncryptedBlteBlocksSkipped`
- The DB2 file may be partially readable or corrupt

```csharp
var storage = await CascStorage.OpenInstallRootAsync(wowPath);

storage.EncryptedBlteBlocksSkipped += (e) =>
{
    Console.WriteLine($"Skipped {e.SkippedBlockCount} encrypted blocks " +
                      $"({e.SkippedLogicalBytes} bytes) for EKey {e.EKey}");
};

var stream = await storage.OpenDb2ByFileDataIdAsync(fileDataId);
// stream may have gaps from skipped encrypted blocks
```

## Performance Considerations

### First Access

Opening CASC storage involves:
1. Reading build configuration (~1-2 seconds)
2. Loading encoding index (~2-5 seconds for large games)
3. Loading root index (~1-3 seconds)

**Total**: ~5-10 seconds for first `OpenInstallRootAsync()` call.

### Subsequent Access

- File reads are fast (direct archive access)
- No additional parsing overhead
- Decompression is efficient (LZ4 is very fast)

### Caching

- Manifest is cached locally after first download
- `LocalFirstManifestProvider` checks cache before downloading
- Cache is reused across sessions

## Troubleshooting

### "shmem file is locked"

```
IOException: The process cannot access the file 'shmem' 
because it is being used by another process.
```

**Cause**: World of Warcraft is running.

**Solution**: Close WoW before opening CASC storage.

### "DB2 file not found by FileDataId"

```
FileNotFoundException: DB2 not found by FileDataId: 123456
```

**Causes:**
1. FileDataID doesn't exist in this WoW version
2. Manifest is outdated
3. File was removed in a patch

**Solutions:**
- Update manifest: Delete cached `manifest.json` and re-download
- Verify FileDataID is correct for your WoW version
- Check WoWDBDefs repository for table availability

### "Build config did not include an ENCODING EKey"

```
NotSupportedException: Build config did not include an ENCODING EKey
```

**Cause**: Unusual WoW installation or unsupported client version.

**Solution**: Ensure you're using a retail/classic WoW installation, not PTR or beta.

### Slow Performance

If CASC access is slow:

1. **SSD**: Store WoW on an SSD for faster I/O
2. **Antivirus**: Exclude WoW directory from real-time scanning
3. **Caching**: Reuse `CascStorage` instance instead of reopening

## Advantages Over FileSystem Provider

| Feature | CASC Provider | FileSystem Provider |
|---------|--------------|---------------------|
| Extraction Required | ❌ No | ✅ Yes |
| Disk Space | Minimal (uses game files) | Large (extracted copies) |
| Version Sync | Automatic | Manual re-extraction |
| Setup Time | ~10 sec (first open) | Hours (extraction) |
| Query Speed | Fast | Fast |
| WoW Can Run | ❌ No (shmem lock) | ✅ Yes |

**Use CASC when:**
- You want zero setup (besides installing WoW)
- You want automatic updates after patches
- Disk space is limited

**Use FileSystem when:**
- You need to query while WoW is running
- You're working with non-standard DB2 files
- You want simpler debugging (view files directly)

## Related Packages

- **MimironSQL.Contracts**: Core interfaces implemented by this package
- **MimironSQL.Providers.FileSystem**: Alternative filesystem-based provider
- **K4os.Compression.LZ4**: LZ4 decompression
- **CsvHelper**: CSV index parsing

## See Also

- [Root README](../../README.md)
- [Architecture Overview](../../.github/instructions/architecture.md)
- [CASC Format Specification](https://wowdev.wiki/CASC)
- [CASCLib Project](https://github.com/WoW-Tools/CASCLib)
