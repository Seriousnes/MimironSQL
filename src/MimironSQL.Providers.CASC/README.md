# MimironSQL.Providers.CASC

Read DB2 files directly from CASC archives (no extraction required).

## Usage

```csharp
// Setup manifest
var manifestOptions = new WowDb2ManifestOptions
{
    CacheDirectory = @"C:\cache",
    AssetName = "manifest.json"
};

using var httpClient = new HttpClient();
var manifestProvider = new WowDb2ManifestProvider(httpClient, Options.Create(manifestOptions));
var localFirst = new LocalFirstManifestProvider(manifestProvider, Options.Create(manifestOptions));
await localFirst.EnsureManifestExistsAsync();

// Open CASC
var storage = await CascStorage.OpenInstallRootAsync(@"C:\World of Warcraft");
var db2Provider = new CascDBCProvider(storage, localFirst);

// Use with EF Core
var options = new DbContextOptionsBuilder<WoWDb2Context>()
    .UseMimironDb2(db2Provider, dbdProvider, tactKeyProvider)
    .Options;
```

**Note**: WoW must be closed (shmem lock)

Target: .NET 10.0
