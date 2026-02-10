# MimironSQL.Providers.CASC

CASC archive provider for reading DB2 files directly from a World of Warcraft installation.

## Overview

This package provides read-only access to DB2 files stored in the CASC (Content Addressable Storage Container) archive system used by World of Warcraft. It handles the full CASC resolution pipeline — manifest lookup, root/encoding index resolution, local archive access, and BLTE decoding — and exposes the result as an `IDb2StreamProvider` for use with the MimironSQL ecosystem.

Key capabilities:

- Resolves DB2 table names to FileDataIds via a manifest file.
- Navigates the CASC root and encoding indices to locate content and encoding keys.
- Reads data from local `.data` archive files and decodes BLTE-compressed streams (including LZ4-compressed and TACT-encrypted blocks).
- Integrates with `MimironSQL.EntityFrameworkCore` via the `UseCasc()` builder pattern, or registers standalone via `AddCasc()`.

## Installation

Packages are published to GitHub Packages. See the [repository README](https://github.com/Seriousnes/MimironSQL) for feed setup, then install:

```shell
dotnet add package MimironSQL.Providers.CASC
```

## EF Core Integration

Use the `UseCasc()` extension method on `IMimironDb2DbContextOptionsBuilder` to configure the CASC provider within an EF Core context.

### Configuration-based

Bind options directly from `IConfiguration`:

```csharp
services.AddDbContext<MyDbContext>(options =>
    options.UseMimironDb2(db2 =>
        db2.UseCasc(configuration)));
```

### Fluent builder

Configure options explicitly with the fluent API:

```csharp
services.AddDbContext<MyDbContext>(options =>
    options.UseMimironDb2(db2 =>
        db2.UseCasc(casc =>
        {
            casc.WithWowInstallRoot(@"C:\Games\World of Warcraft")
                .WithDbdDefinitions(@"C:\WoWDBDefs\definitions")
                .WithManifest(@"C:\cache", "manifest.json")
                .Apply();
        })));
```

The builder also supports custom provider types:

```csharp
casc.WithDbdProvider<MyCustomDbdProvider>()
    .WithManifestProvider<MyCustomManifestProvider>()
    .Apply();
```

## Standalone DI Registration

For non-EF Core usage, register CASC services directly on `IServiceCollection`:

```csharp
services.AddCasc(configuration);
```

Or pass options explicitly:

```csharp
services.AddCasc(new CascDb2ProviderOptions
{
    WowInstallRoot = @"C:\Games\World of Warcraft",
    ManifestCacheDirectory = @"C:\cache"
});
```

This registers:

| Service | Implementation |
|---|---|
| `IDb2StreamProvider` | `CascStorageService` |
| `IManifestProvider` | `FileSystemManifestProvider` |

## Configuration

Keys are read from the `Casc` section of `IConfiguration`, with a fallback to root-level keys for `WowInstallRoot`:

| Key | Required | Default | Description |
|---|---|---|---|
| `Casc:WowInstallRoot` | Yes | — | Path to the World of Warcraft installation directory. |
| `Casc:ManifestCacheDirectory` | No | Implementation-defined | Directory where the DB2 manifest file is cached. |
| `Casc:ManifestAssetName` | No | `manifest.json` | File name of the manifest asset. |
| `Casc:DbdDefinitionsDirectory` | No* | — | Directory containing WoWDBDefs `.dbd` files. Required when using the `UseCasc(configuration)` EF Core overload. |

Example `appsettings.json`:

```json
{
  "Casc": {
    "WowInstallRoot": "C:\\Games\\World of Warcraft",
    "DbdDefinitionsDirectory": "C:\\WoWDBDefs\\definitions",
    "ManifestCacheDirectory": "C:\\cache"
  }
}
```

## Architecture

When a DB2 stream is requested, the CASC provider executes the following pipeline:

1. **Manifest resolution** — The `IManifestProvider` (default: `FileSystemManifestProvider`) maps a DB2 table name (e.g. `Map`) to a FileDataId by reading a `manifest.json` file.
2. **Root index lookup** — The `CascRootIndex` resolves the FileDataId to a content key (CKey).
3. **Encoding resolution** — The `CascEncodingIndex` maps the CKey to an encoding key (EKey). If no encoding entry exists, the CKey is used directly.
4. **Archive read** — The `CascLocalArchiveReader` locates the EKey in the local `.idx` files and reads the corresponding BLTE-encoded data from a `.data` archive file.
5. **BLTE decoding** — The `BlteDecoder` decompresses the data, handling plain, LZ4-compressed, and TACT-encrypted blocks. Encrypted blocks that cannot be decrypted are skipped with diagnostic events.

The entire pipeline is encapsulated in `CascStorageService`, which implements `IDb2StreamProvider` and lazily initializes on first use.

## Public API

This package intentionally keeps its public surface small:

- `CascDb2ProviderOptions` — configuration record for CASC provider settings.
- `CascDb2ProviderBuilder` — fluent builder for EF Core integration.
- `CascStorageService` — `IDb2StreamProvider` implementation that reads DB2 streams from CASC.
- `FileSystemManifestProvider` — default `IManifestProvider` that reads `manifest.json` from disk.
- `ServiceCollectionExtensions.AddCasc(...)` — DI registration for standalone usage.
- `MimironDb2CascOptionsBuilderExtensions.UseCasc(...)` — EF Core integration entry point.
- `IWowBuildIdentityProvider` / `WowBuildIdentityProvider` — resolves WoW build identity from an install directory.

## License

Licensed under the [MIT License](../../LICENSE.txt).
