# MimironSQL.DbContextGenerator

Roslyn source generator for DbContext and entities from WoWDBDefs.

## Setup

1. Install package:
```bash
dotnet add package MimironSQL.DbContextGenerator
```

2. Create `.env`:
```env
WOW_VERSION=11.0.7.58162
DBD_PATH=C:\WoWDBDefs\definitions
```

3. Add to `.csproj`:
```xml
<ItemGroup>
  <AdditionalFiles Include=".env" />
</ItemGroup>
```

4. Build - `WoWDb2Context` and entities are auto-generated

## Customization

Extend with partial classes:

```csharp
public partial class Map
{
    public bool IsDungeon() => Directory?.Contains("dungeon") ?? false;
}
```

Target: .NET Standard 2.0 (analyzer)
