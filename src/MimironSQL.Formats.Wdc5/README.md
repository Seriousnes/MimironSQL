# MimironSQL.Formats.Wdc5

WDC5 format reader for World of Warcraft DB2 files.

## Usage

```csharp
var format = new Wdc5Format(new Wdc5FileOptions
{
    TactKeyProvider = myTactKeyProvider  // Optional for encrypted files
});

using var stream = File.OpenRead("Map.db2");
using var file = format.OpenFile(stream);

for (int i = 0; i < file.RecordCount; i++)
{
    var row = file.GetRow(i);
    // row.Id, row.RecordData
}
```

## Features

- WDC5 format parsing
- Encrypted section support (requires TACT keys)
- Efficient row access via `RowHandle`

Target: .NET 10.0
