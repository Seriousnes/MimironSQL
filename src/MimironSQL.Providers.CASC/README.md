# MimironSQL.Providers.CASC

Reads DB2 files directly from a World of Warcraft installation via the CASC (Content Addressable Storage Container) archive system. Provides `IDb2StreamProvider` and supporting services for CASC archive access, encoding/index resolution, and BLTE decoding.

## Installation

Packages are published to GitHub Packages. See the repository README for feed setup, then install:

```shell
dotnet add package MimironSQL.Providers.CASC
```

## DI Registration

```csharp
services.AddCasc(configuration);
```

Binds configuration from `IConfiguration` sections and registers:

| Service | Implementation |
|---------|---------------|
| `IDb2StreamProvider` | `CascDBCProvider` |
| `ICascStorageService` | `CascStorageService` |
| `IManifestProvider` | `LocalFirstManifestProvider` (falls back to `WowDb2ManifestProvider`) |

Configuration:

Keys are read from the `Casc` section (with a fallback to root-level keys for `WowInstallRoot`):

| Key | Notes |
|-----|------|
| `Casc:WowInstallRoot` | Path to WoW installation (required) |
| `Casc:ManifestCacheDirectory` | Optional cache directory for `manifest.json` |
| `Casc:ManifestAssetName` | Optional manifest file name (default: `manifest.json`) |
| `Casc:DbdDefinitionsDirectory` | Used by EF Core `UseCasc(configuration)` overload |

## Public API

This package intentionally keeps its public surface small:

- `CascDb2ProviderOptions` (configuration object)
- `ServiceCollectionExtensions.AddCasc(...)`
- `MimironDb2CascOptionsBuilderExtensions.UseCasc(...)` (EF Core integration)
- `IManifestProvider` (in `MimironSQL.Contracts`) is the manifest extension point
