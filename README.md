# MimironSQL

Read-only Entity Framework Core provider for World of Warcraft DB2 files.

## Installation

```bash
dotnet add package MimironSQL.EntityFrameworkCore
dotnet add package MimironSQL.Providers.FileSystem
dotnet add package MimironSQL.Formats.Wdc5
```

## Usage

```csharp
var dbdProvider = new FileSystemDbdProvider(
    new FileSystemDbdProviderOptions(@"C:\WoWDBDefs\definitions"));
var db2Provider = new FileSystemDb2StreamProvider(
    new FileSystemDb2StreamProviderOptions(@"C:\WoW\DBFilesClient"));
var tactKeyProvider = new SimpleTactKeyProvider();

var options = new DbContextOptionsBuilder<WoWDb2Context>()
    .UseMimironDb2(db2Provider, dbdProvider, tactKeyProvider)
    .Options;

using var context = new WoWDb2Context(options);
var maps = context.Map.Take(10).ToList();
```

## Packages

- **MimironSQL.EntityFrameworkCore** - EF Core provider
- **MimironSQL.Providers.FileSystem** - Read from disk
- **MimironSQL.Providers.CASC** - Read from WoW archives
- **MimironSQL.DbContextGenerator** - Auto-generate DbContext
- **MimironSQL.Formats.Wdc5** - WDC5 format reader

Requires: .NET 10.0, [WoWDBDefs](https://github.com/wowdev/WoWDBDefs)

See individual package READMEs for details.
