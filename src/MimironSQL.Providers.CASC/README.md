# MimironSQL.Providers.CASC

Reads DB2 files directly from a World of Warcraft installation via the CASC (Content Addressable Storage Container) archive system. Provides `IDb2StreamProvider` and supporting services for CASC archive access, encoding/index resolution, and BLTE decoding.

## Installation

Packages are published to GitHub Packages. See the repository README for feed setup, then install:

```shell
dotnet add package MimironSQL.Providers.CASC
```

## DI Registration

```csharp
services.AddCascNet(configuration);
```

Binds configuration from `IConfiguration` sections and registers:

| Service | Implementation |
|---------|---------------|
| `IDb2StreamProvider` | `CascDBCProvider` |
| `ICascStorageService` | `CascStorageService` |
| `IManifestProvider` | `LocalFirstManifestProvider` (falls back to `WowDb2ManifestProvider`) |

Configuration sections:

| Section | Options type | Key settings |
|---------|-------------|-------------|
| `CascNet` | `CascNetOptions` | `WowInstallRoot` — path to WoW installation |
| `CascStorage` | `CascStorageOptions` | `EnsureManifestOnOpenInstallRoot` |
| `WowDb2Manifest` | `WowDb2ManifestOptions` | `Owner`, `Repository`, `CacheDirectory` |
| `WowListfile` | `WowListfileOptions` | `Owner`, `Repository`, `DownloadOnStartup` |

## Core Types

### `CascStorage`

Opens a WoW installation and resolves files by content key, encoding key, or FileDataID:

```csharp
var storage = await CascStorage.OpenInstallRootAsync("C:/Games/World of Warcraft");

using var stream = await storage.OpenDb2ByFileDataIdAsync(fileDataId);
```

### `CascDBCProvider`

Wraps `CascStorage` as an `IDb2StreamProvider`, resolving table names to FileDataIDs via the manifest:

```csharp
var provider = new CascDBCProvider(storage, manifestProvider);
using var stream = provider.OpenDb2Stream("Map");
```

### `CascKey`

16-byte content/encoding key with hex parsing and equality:

```csharp
var key = CascKey.ParseHex("0123456789abcdef0123456789abcdef");
```

### BLTE Decoding

```csharp
byte[] decoded = BlteDecoder.Decode(rawBlteBytes);
```

### Build Detection

```csharp
var layout = CascInstallLayoutDetector.Detect("C:/Games/World of Warcraft");
var records = CascBuildInfo.Read(layout.BuildInfoPath);
var config = CascBuildConfigParser.ReadFromFile(configPath);
```
