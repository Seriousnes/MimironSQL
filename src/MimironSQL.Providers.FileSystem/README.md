# MimironSQL.Providers.FileSystem

Filesystem-based providers for DB2 and DBD files.

## Usage

```csharp
// DB2 files
var db2Provider = new FileSystemDb2StreamProvider(
    new FileSystemDb2StreamProviderOptions(@"C:\WoW\DBFilesClient"));

// DBD definitions
var dbdProvider = new FileSystemDbdProvider(
    new FileSystemDbdProviderOptions(@"C:\WoWDBDefs\definitions"));

// TACT keys (optional)
var tactKeys = new SimpleTactKeyProvider();
tactKeys.AddKey(0x1234567890ABCDEF, keyBytes);

// Or from CSV
var tactKeys = new FileSystemTactKeyProvider(
    new FileSystemTactKeyProviderOptions(@"C:\keys.csv"));
```

## CSV Format

```csv
KeyName,Key
FA505078126ACB3E,0102030405060708090A0B0C0D0E0F10
```

Target: .NET 10.0
