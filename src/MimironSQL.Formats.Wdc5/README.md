# MimironSQL.Formats.Wdc5

WDC5 binary format reader for World of Warcraft DB2 files.

## Overview

This package implements `IDb2Format` from `MimironSQL.Contracts` for the WDC5 binary format used by World of Warcraft DB2 files. It handles header parsing, field decoding (including packed/pallet/common compression), sparse and dense record layouts, copy tables, and TACT-encrypted sections.

In most scenarios this package is consumed as a transitive dependency via `MimironSQL.EntityFrameworkCore` and does not need to be referenced directly.

## Installation

```shell
dotnet add package MimironSQL.Formats.Wdc5
```

> **Note:** If you already reference `MimironSQL.EntityFrameworkCore`, this package is included transitively.

## Usage

### Opening a file

```csharp
using var stream = File.OpenRead("Map.db2");
var format = new Wdc5Format();
using var file = format.OpenFile(stream);
```

Or with encryption support:

```csharp
var format = new Wdc5Format(tactKeyProvider: myTactKeyProvider);
using var file = format.OpenFile(stream);
```

To construct a `Wdc5File` directly:

```csharp
var options = new Wdc5FileOptions(TactKeyProvider: myTactKeyProvider);
using var file = new Wdc5File(stream, options);
```

### Reading rows

Enumerate all rows:

```csharp
foreach (var handle in file.EnumerateRowHandles())
{
    int id = file.ReadField<int>(handle, Db2VirtualFieldIndex.Id);
    string name = file.ReadField<string>(handle, 0);
}
```

Look up a row by ID:

```csharp
if (file.TryGetRowHandle(2222, out var handle))
{
    var value = file.ReadField<int>(handle, 1);
}
```

## Key Types

| Type | Kind | Description |
| --- | --- | --- |
| `Wdc5Format` | class | `IDb2Format` implementation — entry point for opening files. Accepts optional `ITactKeyProvider` and `Wdc5FormatOptions`. |
| `Wdc5FormatOptions` | class | Format-level options (e.g., `EagerSparseOffsetTable`). |
| `Wdc5File` | class | Parsed WDC5 file. Implements `IDb2File<RowHandle>` and `IDb2DenseStringTableIndexProvider<RowHandle>`. Also implements `IDisposable`. |
| `Wdc5FileOptions` | record | Options controlling parsing, decryption (TACT key provider, nonce strategy), and record loading mode. |
| `Wdc5Header` | record struct | Parsed file header (schema hash, field counts, flags, section count, etc.). |
| `Wdc5SectionHeader` | record struct | Per-section header (TACT key lookup, record count, string table size, copy table count). |
| `Wdc5Section` | class | Parsed section data including record bytes, string table, index data, and copy table entries. |
| `FieldMetaData` | record struct | Per-field bit width and offset within a record. |
| `ColumnMetaData` | struct | Extended column metadata including compression type, pallet, and common-data parameters. |
| `ColumnCompressionDataImmediate` | record struct | Compression parameters for `Immediate` / `SignedImmediate` columns. |
| `ColumnCompressionDataPallet` | record struct | Compression parameters for `Pallet` / `PalletArray` columns. |
| `ColumnCompressionDataCommon` | record struct | Compression parameters for `Common` columns. |
| `CompressionType` | enum | Column compression mode: `None`, `Immediate`, `Common`, `Pallet`, `PalletArray`, `SignedImmediate`. |
| `SparseEntry` | record struct | Offset/size pair for a sparse (offset-map) record entry. |
| `Wdc5FileLookupTracker` | static class | Diagnostic helper for tracking `TryGetRowById` call counts. |
| `Wdc5FileLookupSnapshot` | record struct | Snapshot of lookup tracker state. |

## Header Structure

### `Wdc5Header`

| Field | Type | Description |
| --- | --- | --- |
| `SchemaVersion` | `uint` | Schema version from the file header. |
| `SchemaString` | `string` | Schema string identifier. |
| `RecordsCount` | `int` | Total number of records. |
| `FieldsCount` | `int` | Number of logical fields per record. |
| `RecordSize` | `int` | Size of each record in bytes. |
| `StringTableSize` | `int` | Size of the string table in bytes. |
| `TableHash` | `uint` | Table hash. |
| `LayoutHash` | `uint` | Layout hash. |
| `MinIndex` / `MaxIndex` | `int` | Record index range. |
| `Locale` | `int` | Locale identifier. |
| `Flags` | `Db2Flags` | DB2 flags (e.g., sparse, has offset map). |
| `IdFieldIndex` | `ushort` | Zero-based index of the ID field. |
| `TotalFieldsCount` | `int` | Total fields including hidden fields. |
| `PackedDataOffset` | `int` | Offset of packed data. |
| `LookupColumnCount` | `int` | Number of lookup columns. |
| `ColumnMetaDataSize` | `int` | Size of column metadata. |
| `CommonDataSize` | `int` | Size of common data. |
| `PalletDataSize` | `int` | Size of pallet data. |
| `SectionsCount` | `int` | Number of sections in the file. |

### `Wdc5SectionHeader`

| Field | Type | Description |
| --- | --- | --- |
| `TactKeyLookup` | `ulong` | TACT key lookup identifier (`0` for unencrypted sections). |
| `FileOffset` | `int` | Absolute file offset where the section begins. |
| `NumRecords` | `int` | Number of records in the section. |
| `StringTableSize` | `int` | String table size for the section. |
| `OffsetRecordsEndOffset` | `int` | End offset of offset-map records. |
| `IndexDataSize` | `int` | Index data size in bytes. |
| `ParentLookupDataSize` | `int` | Size of parent lookup data. |
| `OffsetMapIDCount` | `int` | Number of offset-map ID entries. |
| `CopyTableCount` | `int` | Number of copy-table entries. |

## Encryption Support

WDC5 files may contain TACT-encrypted sections. To decrypt them, supply an `ITactKeyProvider` via `Wdc5FileOptions`:

```csharp
var options = new Wdc5FileOptions(
    TactKeyProvider: myTactKeyProvider,
    EncryptedRowNonceStrategy: Wdc5EncryptedRowNonceStrategy.SourceId);

using var file = new Wdc5File(stream, options);
```

The `Wdc5EncryptedRowNonceStrategy` enum controls how per-row decryption nonces are derived:

| Value | Description |
| --- | --- |
| `SourceId` | Use the source row ID from the raw record (default). |
| `DestinationId` | Use the destination (post-copy) row ID. |

The `Wdc5RecordLoadingMode` enum controls when record data is loaded:

| Value | Description |
| --- | --- |
| `Lazy` | Records are read on demand (default). |
| `Eager` | All record data is loaded upfront during file construction. |

Decryption is performed via the `Salsa20` cipher bundled as a project dependency.

## License

[MIT](../../LICENSE.txt)
