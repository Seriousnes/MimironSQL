# MimironSQL.Dbd

DBD file parser for WoWDBDefs schema definitions.

## Usage

```csharp
var dbdFile = DbdFile.Parse(File.OpenRead("Map.dbd"));

// Access columns
foreach (var column in dbdFile.Columns.Values)
{
    Console.WriteLine($"{column.Name}: {column.Type}");
    if (column.ForeignTable != null)
        Console.WriteLine($"  → {column.ForeignTable}.{column.ForeignColumn}");
}

// Access layouts
foreach (var layout in dbdFile.Layouts)
{
    // Build-specific field layouts
}
```

Used internally by EF Core provider and source generator.

Target: .NET Standard 2.0
