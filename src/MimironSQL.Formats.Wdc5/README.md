# MimironSQL.Formats.Wdc5

WDC5 binary format reader for World of Warcraft DB2 files. Implements `IDb2Format` from `MimironSQL.Contracts`.

## Usage

### Opening a file

```csharp
using var stream = File.OpenRead("Map.db2");
var file = new Wdc5Format().OpenFile(stream);
```

Or with encryption support:

```csharp
var options = new Wdc5FileOptions(TactKeyProvider: myTactKeyProvider);
var file = new Wdc5File(stream, options);
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

### Registering with the format registry

```csharp
var registry = new Db2FormatRegistry();
Wdc5Format.Register(registry);
```

## Public API

### `Wdc5Format`

```csharp
public sealed class Wdc5Format : IDb2Format
{
    public Db2Format Format { get; }

    public static void Register(Db2FormatRegistry registry);
    public IDb2File OpenFile(Stream stream);
    public Db2FileLayout GetLayout(IDb2File file);
}
```

### `Wdc5File`

Implements `IDb2File<RowHandle>` and `IDb2DenseStringTableIndexProvider<RowHandle>`.

```csharp
public sealed class Wdc5File
{
    public Wdc5File(Stream stream);
    public Wdc5File(Stream stream, Wdc5FileOptions? options);

    public Wdc5Header Header { get; }
    public IReadOnlyList<Wdc5SectionHeader> Sections { get; }
    public IReadOnlyList<Wdc5Section> ParsedSections { get; }
    public FieldMetaData[] FieldMeta { get; }
    public ColumnMetaData[] ColumnMeta { get; }
    public Db2Flags Flags { get; }
    public int RecordsCount { get; }

    public IEnumerable<RowHandle> EnumerateRowHandles();
    public IEnumerable<RowHandle> EnumerateRows();
    public T ReadField<T>(RowHandle handle, int fieldIndex);
    public void ReadAllFields(RowHandle handle, Span<object> values);
    public bool TryGetRowHandle<TId>(TId id, out RowHandle handle);
    public bool TryGetRowById<TId>(TId id, out RowHandle row);
    public bool TryGetDenseStringTableIndex(RowHandle row, int fieldIndex, out int stringTableIndex);
}
```

### `Wdc5FileOptions`

```csharp
public sealed record Wdc5FileOptions(
    ITactKeyProvider? TactKeyProvider = null,
    Wdc5EncryptedRowNonceStrategy EncryptedRowNonceStrategy = Wdc5EncryptedRowNonceStrategy.SourceId);
```

### `Wdc5EncryptedRowNonceStrategy`

Controls how the nonce is derived when decrypting encrypted rows.

```csharp
public enum Wdc5EncryptedRowNonceStrategy { DestinationId, SourceId }
```

### Header / Section Types

```csharp
public readonly record struct Wdc5Header(
    uint SchemaVersion, string SchemaString, int RecordsCount, int FieldsCount,
    int RecordSize, int StringTableSize, uint TableHash, uint LayoutHash,
    int MinIndex, int MaxIndex, int Locale, Db2Flags Flags, ushort IdFieldIndex,
    int TotalFieldsCount, int PackedDataOffset, int LookupColumnCount,
    int ColumnMetaDataSize, int CommonDataSize, int PalletDataSize, int SectionsCount);

public readonly record struct Wdc5SectionHeader(
    ulong TactKeyLookup, int FileOffset, int NumRecords, int StringTableSize,
    int OffsetRecordsEndOffset, int IndexDataSize, int ParentLookupDataSize,
    int OffsetMapIDCount, int CopyTableCount);
```

### Field Metadata

```csharp
public readonly record struct FieldMetaData(short Bits, short Offset);

public struct ColumnMetaData
{
    public ushort RecordOffset;
    public ushort Size;
    public uint AdditionalDataSize;
    public CompressionType CompressionType;
    public ColumnCompressionDataImmediate Immediate;
    public ColumnCompressionDataPallet Pallet;
    public ColumnCompressionDataCommon Common;
}

public enum CompressionType : uint
{
    None, Immediate, Common, Pallet, PalletArray, SignedImmediate
}
```

### Lookup Tracking

```csharp
public static class Wdc5FileLookupTracker
{
    public static IDisposable Start();
    public static Wdc5FileLookupSnapshot Snapshot();
}

public readonly record struct Wdc5FileLookupSnapshot(int TotalTryGetRowByIdCalls);
```
