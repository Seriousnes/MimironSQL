# MimironSQL

Read-only Entity Framework Core provider for World of Warcraft DB2 files.

## Installation

### Required Packages

```bash
# Core EF Core provider - provides UseMimironDb2() extension and query engine
dotnet add package MimironSQL.EntityFrameworkCore

# Format reader - parses WDC5 binary format
dotnet add package MimironSQL.Formats.Wdc5

# Data source provider - choose one:
dotnet add package MimironSQL.Providers.FileSystem  # Read from extracted files
dotnet add package MimironSQL.Providers.CASC        # Read directly from WoW archives
```

### Optional Packages

```bash
# Source generator - auto-generates DbContext and entities from WoWDBDefs
dotnet add package MimironSQL.DbContextGenerator
```

## Basic Usage

```csharp
using Microsoft.EntityFrameworkCore;
using MimironSQL.EntityFrameworkCore;
using MimironSQL.Providers;

// Configure providers
var dbdProvider = new FileSystemDbdProvider(
    new FileSystemDbdProviderOptions(@"C:\WoWDBDefs\definitions"));
var db2Provider = new FileSystemDb2StreamProvider(
    new FileSystemDb2StreamProviderOptions(@"C:\WoW\DBFilesClient"));
var tactKeyProvider = new SimpleTactKeyProvider();

// Configure EF Core
var options = new DbContextOptionsBuilder<WoWDb2Context>()
    .UseMimironDb2(db2Provider, dbdProvider, tactKeyProvider)
    .Options;

using var context = new WoWDb2Context(options);
var maps = context.Map.Where(m => m.Id < 100).ToList();
```

## Dependency Injection Configuration

```csharp
using Microsoft.Extensions.DependencyInjection;
using MimironSQL.EntityFrameworkCore;
using MimironSQL.Providers;

var services = new ServiceCollection();

// Register providers
services.AddSingleton<IDb2StreamProvider>(sp => 
    new FileSystemDb2StreamProvider(
        new FileSystemDb2StreamProviderOptions(@"C:\WoW\DBFilesClient")));

services.AddSingleton<IDbdProvider>(sp => 
    new FileSystemDbdProvider(
        new FileSystemDbdProviderOptions(@"C:\WoWDBDefs\definitions")));

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

## Package Overview

| Package | Purpose |
|---------|---------|
| **MimironSQL.EntityFrameworkCore** | EF Core database provider, query engine |
| **MimironSQL.Formats.Wdc5** | WDC5 format parser (binary → rows) |
| **MimironSQL.Providers.FileSystem** | Read DB2/DBD files from disk |
| **MimironSQL.Providers.CASC** | Read DB2 files from WoW CASC archives |
| **MimironSQL.DbContextGenerator** | Generate DbContext from WoWDBDefs |
| **MimironSQL.Contracts** | Extension interfaces for custom implementations |

## Requirements

- .NET 10.0 or later
- [WoWDBDefs](https://github.com/wowdev/WoWDBDefs) schema definitions
- DB2 files (extracted or via CASC)

## Supported LINQ Operations

✅ Where, Select, Include, ThenInclude, Take, Skip, First, Single, Count, Any, All  
❌ Async queries, SaveChanges, GroupBy, Join

See package-specific READMEs for detailed API documentation.
