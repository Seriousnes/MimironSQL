# MimironSQL.Providers.FileSystem

File system-based data providers for reading World of Warcraft DB2 files, DBD definitions, and TACT encryption keys from local disk.

## Overview

This package implements the three provider interfaces defined in `MimironSQL.Contracts`:

- `IDb2StreamProvider` — opens DB2 file streams from a directory.
- `IDbdProvider` — parses WoWDBDefs `.dbd` definition files from a directory.
- `ITactKeyProvider` — loads TACT encryption keys from a CSV file.

Each provider expects a specific file naming convention on disk (see [File Conventions](#file-conventions) below). Use this package when working with extracted or exported WoW data files on the local file system.

## Installation

Packages are published to GitHub Packages. See the [repository README](https://github.com/Seriousnes/MimironSQL#readme) for feed setup, then install:

```shell
dotnet add package MimironSQL.Providers.FileSystem
```

## EF Core Integration

When used with `MimironSQL.EntityFrameworkCore`, configure the file system provider via `UseFileSystem(...)` on the options builder:

```csharp
var builder = new DbContextOptionsBuilder<WoWDb2Context>();

builder.UseMimironDb2(o => o.UseFileSystem(
    db2DirectoryPath: "path/to/db2/files",
    dbdDefinitionsDirectory: "path/to/dbd/definitions"));

using var context = new WoWDb2Context(builder.Options);
```

An overload accepting `FileSystemDb2StreamProviderOptions` and `FileSystemDbdProviderOptions` is also available for more explicit configuration.

## Standalone Usage

The three providers can be used independently of EF Core.

### FileSystemDb2StreamProvider

Opens a DB2 file stream from a directory on disk. Expects files named `{tableName}.db2`.

```csharp
var provider = new FileSystemDb2StreamProvider(
    new FileSystemDb2StreamProviderOptions("path/to/db2/files"));

using var stream = provider.OpenDb2Stream("Map");
```

### FileSystemDbdProvider

Parses a `.dbd` file from a directory on disk. Expects files named `{tableName}.dbd`.

```csharp
var parser = new DbdParser();

var provider = new FileSystemDbdProvider(
    new FileSystemDbdProviderOptions("path/to/dbd/definitions"),
    parser);

IDbdFile dbd = provider.Open("Map");
```

### FileSystemTactKeyProvider

Loads TACT encryption keys from a CSV file (lookup → key hex pairs).

```csharp
var provider = new FileSystemTactKeyProvider(
    new FileSystemTactKeyProviderOptions("path/to/tactkeys.csv"));

if (provider.TryGetKey(0x1234567890ABCDEF, out var key))
{
    // key is a 16-byte ReadOnlyMemory<byte>
}
```

## File Conventions

| File type | Expected pattern | Example |
| --- | --- | --- |
| DB2 data file | `{tableName}.db2` in a flat directory | `Map.db2` |
| DBD definition | `{tableName}.dbd` in a flat directory | `Map.dbd` |
| TACT keys | Single CSV file (lookup,key hex pairs) | `tactkeys.csv` |

## Public API

| Class | Options type | Implements |
| --- | --- | --- |
| `FileSystemDb2StreamProvider` | `FileSystemDb2StreamProviderOptions(string Db2DirectoryPath)` | `IDb2StreamProvider` |
| `FileSystemDbdProvider` | `FileSystemDbdProviderOptions(string DefinitionsDirectory)` | `IDbdProvider` |
| `FileSystemTactKeyProvider` | `FileSystemTactKeyProviderOptions(string KeyFilePath)` | `ITactKeyProvider` |

## License

Licensed under the [MIT License](https://github.com/Seriousnes/MimironSQL/blob/main/LICENSE.txt).
