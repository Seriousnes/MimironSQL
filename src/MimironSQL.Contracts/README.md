# MimironSQL.Contracts

Public interfaces and types for extending MimironSQL with custom formats, providers, and data sources.

## Providers

### `IDb2StreamProvider`

Opens a raw byte stream for a named DB2 table.

```csharp
public interface IDb2StreamProvider
{
    Stream OpenDb2Stream(string tableName);
}
```

### `IDbdProvider`

Provides parsed DBD metadata for a named table.

```csharp
public interface IDbdProvider
{
    IDbdFile Open(string tableName);
}
```

### `ITactKeyProvider`

Resolves TACT encryption keys by their 8-byte lookup ID.

```csharp
public interface ITactKeyProvider
{
    bool TryGetKey(ulong tactKeyLookup, out ReadOnlyMemory<byte> key);
}
```

## Format Interfaces

### `IDb2Format`

Reads a binary DB2 stream and produces an `IDb2File`.

```csharp
public interface IDb2Format
{
    Db2Format Format { get; }
    IDb2File OpenFile(Stream stream);
    Db2FileLayout GetLayout(IDb2File file);
}
```

Register custom formats with `Db2FormatRegistry`:

```csharp
var registry = new Db2FormatRegistry();
registry.Register(myCustomFormat);
```

### `IDb2File` / `IDb2File<TRow>`

Represents an opened DB2 file. Provides row enumeration, field reads, and row-by-ID lookups.

```csharp
public interface IDb2File
{
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

### `RowHandle`

Identifies a single row within a DB2 file by section index, row offset, and row ID.

```csharp
public readonly struct RowHandle(int sectionIndex, int rowIndexInSection, int rowId);
```

### `Db2FileLayout`

Layout hash and physical field count for a DB2 file (used to match against DBD definitions).

```csharp
public readonly struct Db2FileLayout(uint layoutHash, int physicalFieldsCount);
```

## DBD Interfaces

The DBD interfaces model the parsed content of a WoWDBDefs `.dbd` file:

| Interface | Purpose |
|-----------|---------|
| `IDbdFile` | Root — columns by name, layouts, and global builds |
| `IDbdLayout` | Layout hashes and associated build blocks |
| `IDbdBuildBlock` | A single build's field list |
| `IDbdLayoutEntry` | One field in a build — name, type, array count, flags |
| `IDbdColumn` | Column-level metadata — value type, foreign key reference, verified flag |

## Enums

| Enum | Values |
|------|--------|
| `Db2Format` | `Unknown`, `Wdc3`, `Wdc4`, `Wdc5` |
| `Db2Flags` | `None`, `Sparse`, `SecondaryKey`, `Index`, `BitPacked` |
| `Db2ValueType` | `Unknown`, `Int64`, `UInt64`, `Single`, `String`, `LocString` |

## Utilities

### `Db2FormatDetector`

Detects the DB2 format version from the first few header bytes:

```csharp
Db2Format format = Db2FormatDetector.Detect(headerBytes);
Db2Format format = Db2FormatDetector.DetectOrThrow(headerBytes); // throws on Unknown
```
