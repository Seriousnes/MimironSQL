# MimironSQL.Contracts

Public interfaces, value types, and extension points for the MimironSQL ecosystem.

## Overview

`MimironSQL.Contracts` defines the abstractions that MimironSQL uses to read DB2 files, resolve DBD metadata, and detect binary formats. Reference this package directly when implementing a custom format reader, stream provider, or DBD provider. In most scenarios you do not need to reference it explicitly — it is a transitive dependency of the higher-level packages such as `MimironSQL.EntityFrameworkCore`, `MimironSQL.Formats.Wdc5`, and `MimironSQL.Providers.FileSystem`.

## Installation

```shell
dotnet add package MimironSQL.Contracts --version 0.1.0
```

> **Note:** This package is published to GitHub Packages. If you consume one of the higher-level MimironSQL packages, this dependency is pulled in transitively.

## Provider Interfaces

### IDb2StreamProvider

Opens a raw byte stream for a named DB2 table.

```csharp
public interface IDb2StreamProvider
{
    Stream OpenDb2Stream(string tableName);
    Task<Stream> OpenDb2StreamAsync(string tableName, CancellationToken cancellationToken = default);
}
```

### IDbdProvider

Provides parsed DBD metadata for a named table.

```csharp
public interface IDbdProvider
{
    IDbdFile Open(string tableName);
}
```

### IDbdParser

Parses WoWDBDefs `.dbd` content into the contracts-level DBD model.

```csharp
public interface IDbdParser
{
    IDbdFile Parse(Stream stream);
    IDbdFile Parse(string path);
}
```

### ITactKeyProvider

Resolves TACT encryption keys by their 8-byte lookup ID.

```csharp
public interface ITactKeyProvider
{
    bool TryGetKey(ulong tactKeyLookup, out ReadOnlyMemory<byte> key);
}
```

### IManifestProvider

Resolves DB2 table names or CASC paths to FileDataIds.

```csharp
public interface IManifestProvider
{
    Task EnsureManifestExistsAsync(CancellationToken cancellationToken = default);
    Task<int?> TryResolveDb2FileDataIdAsync(string db2NameOrPath, CancellationToken cancellationToken = default);
}
```

## Format Interfaces

### IDb2Format

Reads a binary DB2 stream and produces an `IDb2File`.

```csharp
public interface IDb2Format
{
    Db2Format Format { get; }
    IDb2File OpenFile(Stream stream);
    Db2FileLayout GetLayout(IDb2File file);
    Db2FileLayout GetLayout(Stream stream);
}
```

### IDb2FileHeader

Provides the parts of a DB2 file header needed by the query engine.

```csharp
public interface IDb2FileHeader
{
    uint LayoutHash { get; }
    int FieldsCount { get; }
}
```

### IDb2File / IDb2File\<TRow\>

Represents an opened DB2 file. Provides row enumeration, field reads, and row-by-ID lookups.

```csharp
public interface IDb2File : IDisposable
{
    IDb2FileHeader Header { get; }
    Type RowType { get; }
    Db2Flags Flags { get; }
    int RecordsCount { get; }
    ReadOnlyMemory<byte> DenseStringTableBytes { get; }

    IEnumerable<RowHandle> EnumerateRowHandles();
    T ReadField<T>(RowHandle handle, int fieldIndex);
    bool TryGetRowHandle<TId>(TId id, out RowHandle handle)
        where TId : IEquatable<TId>, IComparable<TId>;
}

public interface IDb2File<TRow> : IDb2File where TRow : struct
{
    IEnumerable<TRow> EnumerateRows();
    bool TryGetRowById<TId>(TId id, out TRow row)
        where TId : IEquatable<TId>, IComparable<TId>;
}
```

### IDb2DenseStringTableIndexProvider\<TRow\>

Resolves dense string table indexes for row fields.

```csharp
public interface IDb2DenseStringTableIndexProvider<TRow> where TRow : struct
{
    bool TryGetDenseStringTableIndex(TRow row, int fieldIndex, out int stringTableIndex);
}
```

## DBD Interfaces

The DBD interfaces model the parsed content of a WoWDBDefs `.dbd` file:

| Interface | Purpose |
|---|---|
| `IDbdFile` | Root — columns by name, layouts, global builds, and layout lookup |
| `IDbdLayout` | Layout hashes, associated build blocks, and build selection by physical column count |
| `IDbdBuildBlock` | A single build's field list and physical column count |
| `IDbdLayoutEntry` | One field in a build — name, type, array count, flags, and inline type token |
| `IDbdColumn` | Column-level metadata — value type, foreign key reference, verified flag |

## Value Types

### RowHandle

Identifies a single row within a DB2 file by section index, row offset, and row ID. Implements `IRowHandle`.

```csharp
public readonly struct RowHandle(int sectionIndex, int rowIndexInSection, int rowId) : IRowHandle;
```

### IRowHandle

Provides access to a stable `RowHandle` for row-like objects.

```csharp
public interface IRowHandle
{
    RowHandle Handle { get; }
}
```

### Db2FileLayout

Layout hash and physical field count for a DB2 file (used to match against DBD definitions).

```csharp
public readonly struct Db2FileLayout(uint layoutHash, int physicalFieldsCount);
```

## Enums

| Enum | Values |
|---|---|
| `Db2Format` | `Unknown`, `Wdc3`, `Wdc4`, `Wdc5` |
| `Db2Flags` | `None`, `Sparse`, `SecondaryKey`, `Index`, `BitPacked` |
| `Db2ValueType` | `Unknown`, `Int64`, `UInt64`, `Single`, `String`, `LocString` |

## Utilities

### Db2FormatDetector

Detects the DB2 format version from the first few header bytes.

```csharp
Db2Format format = Db2FormatDetector.Detect(headerBytes);
Db2Format format = Db2FormatDetector.DetectOrThrow(headerBytes); // throws on Unknown
```

### Db2VirtualFieldIndex

Defines reserved virtual field indexes for the query engine.

```csharp
public static class Db2VirtualFieldIndex
{
    public const int Id = -1;
    public const int ParentRelation = -2;
    public const int UnsupportedNonInline = -3;
}
```

## EF Core Contracts

### IMimironDb2DbContextOptionsBuilder

Abstraction for configuring the MimironSQL DB2 provider when building an EF Core `DbContext`.

```csharp
public interface IMimironDb2DbContextOptionsBuilder
{
    IMimironDb2DbContextOptionsBuilder WithWowVersion(string wowVersion);
    IMimironDb2DbContextOptionsBuilder ConfigureProvider(
        string providerKey,
        int providerConfigHash,
        Action<IServiceCollection> applyProviderServices);
}
```

## License

This project is licensed under the [MIT License](https://github.com/Seriousnes/MimironSQL/blob/main/LICENSE.txt).
