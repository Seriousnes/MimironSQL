# MimironSQL.Contracts

Core interfaces for extending MimironSQL.

## Key Interfaces

**IDb2Format** - Implement custom DB2 format readers
```csharp
public interface IDb2Format
{
    Db2Format Format { get; }
    IDb2File OpenFile(Stream stream);
    Db2FileLayout GetLayout(IDb2File file);
}
```

**IDb2StreamProvider** - Provide DB2 file streams
```csharp
public interface IDb2StreamProvider
{
    Stream OpenDb2Stream(string tableName);
}
```

**IDbdProvider** - Provide DBD schema files
```csharp
public interface IDbdProvider
{
    IDbdFile Open(string tableName);
}
```

**ITactKeyProvider** - Provide TACT encryption keys
```csharp
public interface ITactKeyProvider
{
    bool TryGetKey(ulong keyName, out ReadOnlyMemory<byte> key);
}
```

Target: .NET Standard 2.0
