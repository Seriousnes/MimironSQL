# MimironSQL.Providers.CASC

CASC (Content Addressable Storage Container) providers for reading DB2 files and DBD definitions directly from WoW's game archives. Enables querying without extracting files to disk.

## Overview

`MimironSQL.Providers.CASC` provides access to DB2 files stored in Blizzard's CASC storage format. This is the native storage format used by modern World of Warcraft clients.

**Status**: 🚧 Work in Progress

This package is currently under development. The CASC provider type is not yet fully supported in the EF Core integration.

## Package Information

- **Package ID**: N/A (not packaged)
- **Target Framework**: .NET 10.0
- **IsPackable**: No
- **Dependencies**:
  - `K4os.Compression.LZ4` (for decompression)
  - `CsvHelper` (for parsing CSV indices)
  - `Microsoft.Extensions.Configuration.Abstractions`
  - `Microsoft.Extensions.DependencyInjection.Abstractions`
  - `Microsoft.Extensions.Http`
  - `Microsoft.Extensions.Options`
  - `MimironSQL.Contracts`

## Architecture & Design

### CASC Storage Overview

CASC is Blizzard's Content Addressable Storage Container format used for game file storage:

- **Content Addressing**: Files are stored by content hash, not by path
- **Chunked Storage**: Large files are split into chunks
- **Compression**: Data is compressed with LZ4 or other algorithms
- **Encryption**: Some files are encrypted with Blizzard encryption keys
- **Indices**: CSV-based indices map file paths to content hashes

### Key Components

#### `CascStorage`

Main entry point for accessing CASC archives.

```csharp
public sealed class CascStorage : IDisposable
{
    // Opens CASC storage from a directory
    public static CascStorage Open(string cascRootPath);
    
    // Retrieves a file by path
    public Stream OpenFile(string filePath);
    
    // Checks if a file exists
    public bool FileExists(string filePath);
}
```

#### `CascPath`

Represents a logical file path in CASC storage.

```csharp
public readonly struct CascPath
{
    public string Path { get; }
    public ulong Hash { get; }
}
```

#### `CascBucket`

Represents a content bucket (chunk) in CASC storage.

```csharp
public sealed class CascBucket
{
    public byte[] ContentKey { get; }
    public byte[] EncodingKey { get; }
    public int Size { get; }
}
```

## Planned Public API

Once development is complete, the package will provide:

### CASC DB2 Stream Provider

```csharp
public sealed class CascDb2StreamProvider : IDb2StreamProvider
{
    public CascDb2StreamProvider(CascDb2StreamProviderOptions options);
    public Stream OpenDb2Stream(string tableName);
}

public sealed class CascDb2StreamProviderOptions
{
    public required string CascRootPath { get; init; }
    public string? Locale { get; init; } = "enUS";
}
```

**Planned Usage:**
```csharp
var options = new CascDb2StreamProviderOptions
{
    CascRootPath = @"C:\World of Warcraft\Data",
    Locale = "enUS"
};

var provider = new CascDb2StreamProvider(options);
using var stream = provider.OpenDb2Stream("Map");
```

### CASC DBD Provider

```csharp
public sealed class CascDbdProvider : IDbdProvider
{
    public CascDbdProvider(CascDbdProviderOptions options);
    public IDbdFile Open(string tableName);
}

public sealed class CascDbdProviderOptions
{
    public required string WoWDBDefsUrl { get; init; }
    public string? CachePath { get; init; }
}
```

**Planned Usage:**
```csharp
var options = new CascDbdProviderOptions
{
    WoWDBDefsUrl = "https://github.com/wowdev/WoWDBDefs/raw/master/definitions/",
    CachePath = @"C:\Temp\DBDCache"  // Optional local cache
};

var provider = new CascDbdProvider(options);
var dbdFile = provider.Open("Map");
```

## Integration with Overall Design

### CASC in the Data Pipeline

```
User Query
    ↓
EF Core Provider
    ↓
MimironSQL Query Engine
    ↓
Format Reader (WDC5)
    ↓
CASC Provider ← (reads from CASC archives)
    ↓
CASC Storage (compressed, indexed)
```

### Advantages Over FileSystem Provider

1. **No Extraction Required**: Read directly from game archives
2. **Automatic Updates**: Always reads from current game installation
3. **Space Efficient**: No need to maintain extracted copies
4. **Version Accuracy**: Guaranteed to match the installed game version

### Implementation Challenges

The CASC provider faces several technical challenges:

1. **Complex Format**: CASC has multiple layers (encoding, indices, buckets)
2. **Compression**: Requires LZ4 and potentially other decompression algorithms
3. **Encryption**: Some files are encrypted with Blizzard keys
4. **Index Parsing**: CSV-based indices must be parsed and cached
5. **Performance**: Random access patterns need optimization

## Current Implementation Status

### Completed

- ✅ Basic CASC storage structure models
- ✅ Path and bucket abstractions
- ✅ Interface definitions

### In Progress

- 🚧 CASC archive reading
- 🚧 Index parsing and caching
- 🚧 File decompression pipeline
- 🚧 Content hash resolution

### Not Yet Implemented

- ❌ Complete DB2 stream provider
- ❌ DBD provider with HTTP fallback
- ❌ Encryption support
- ❌ Performance optimizations
- ❌ Service registration with EF Core

## Development Roadmap

### Phase 1: Core CASC Reading ✅

- Basic file structure models
- Path and hash abstractions

### Phase 2: Archive Access 🚧

- Index file parsing
- Content key resolution
- Bucket retrieval

### Phase 3: Decompression 🚧

- LZ4 decompression
- Stream handling
- Buffer management

### Phase 4: Provider Implementation ⏳

- `CascDb2StreamProvider`
- File caching strategies
- Error handling

### Phase 5: Integration ⏳

- Service registration
- EF Core integration
- Configuration options

### Phase 6: Optimization ⏳

- Index caching
- Parallel loading
- Memory efficiency

## How to Contribute

The CASC provider is a complex component that would benefit from community contributions:

### Areas for Contribution

1. **CASC Format Expertise**: Knowledge of CASC internals
2. **Decompression**: Optimizing LZ4 and other algorithms
3. **Index Parsing**: Efficient CSV and binary index handling
4. **Testing**: Real-world CASC archive testing
5. **Documentation**: CASC format documentation

### Getting Started

1. Review CASC format documentation on wowdev.wiki
2. Explore existing C# CASC implementations (CASCLib)
3. Understand the provider interfaces in `MimironSQL.Contracts`
4. Implement missing functionality incrementally
5. Add comprehensive tests

## Related Projects

### CASCLib

CASCLib is a mature C# library for CASC reading:
- GitHub: [https://github.com/WoW-Tools/CASCLib](https://github.com/WoW-Tools/CASCLib)
- May be used as reference or dependency

### CASC Format Documentation

- [wowdev.wiki - CASC](https://wowdev.wiki/CASC)
- [CASCExplorer](https://github.com/WoW-Tools/CASCExplorer)

## Workarounds Until Complete

Until the CASC provider is fully implemented, use the FileSystem provider:

### Extract DB2 Files First

```bash
# Use CASCExplorer or similar tool to extract DB2 files
# Extract to: C:\ExtractedDB2\DBFilesClient\
```

### Use FileSystem Provider

```csharp
var db2Provider = new FileSystemDb2StreamProvider(
    new FileSystemDb2StreamProviderOptions
    {
        Db2DirectoryPath = @"C:\ExtractedDB2\DBFilesClient"
    });
```

## Testing Strategy

When the CASC provider is implemented, it will be tested against:

1. **Real CASC Archives**: From actual WoW installations
2. **Various Versions**: Retail, Classic, PTR builds
3. **Edge Cases**: Encrypted files, large files, corrupted indices
4. **Performance**: Benchmarks vs FileSystem provider
5. **Compatibility**: Cross-platform behavior

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
