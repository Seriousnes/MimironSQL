# MimironSQL.Providers.FileSystem

File system-based implementations of the three provider interfaces from `MimironSQL.Contracts`. Reads DB2 files, DBD definitions, and TACT keys from local disk.

This is an internal library — not published as a standalone package.

## Providers

### `FileSystemDb2StreamProvider`

Opens a DB2 file stream from a directory on disk. Expects files named `{tableName}.db2`.

```csharp
var provider = new FileSystemDb2StreamProvider(
    new FileSystemDb2StreamProviderOptions("path/to/db2/files"));

using var stream = provider.OpenDb2Stream("Map");
```

### `FileSystemDbdProvider`

Parses a `.dbd` file from a directory on disk. Expects files named `{tableName}.dbd`.

```csharp
var parser = new DbdParser();

var provider = new FileSystemDbdProvider(
    new FileSystemDbdProviderOptions("path/to/dbd/definitions"),
    parser);

IDbdFile dbd = provider.Open("Map");
```

### `FileSystemTactKeyProvider`

Loads TACT encryption keys from a CSV file (lookup → key hex pairs).

```csharp
var provider = new FileSystemTactKeyProvider(
    new FileSystemTactKeyProviderOptions("path/to/tactkeys.csv"));

if (provider.TryGetKey(0x1234567890ABCDEF, out var key))
{
    // key is a 16-byte ReadOnlyMemory<byte>
}
```
