# MimironSQL.Dbd

Parser for WoWDBDefs `.dbd` definition files, producing a strongly-typed model used by the MimironSQL query engine.

## Overview

`MimironSQL.Dbd` reads `.dbd` files from the [WoWDBDefs](https://github.com/wowdev/WoWDBDefs) project and parses them into a typed object model (`DbdFile`, `DbdLayout`, `DbdBuildBlock`, `DbdLayoutEntry`, `DbdColumn`). These types implement the corresponding interfaces defined in `MimironSQL.Contracts` (`IDbdFile`, `IDbdLayout`, etc.).

This is an **internal library** — it is not published as a standalone NuGet package. It is shipped as an embedded dependency inside:

- `MimironSQL.EntityFrameworkCore` — included under `lib/`
- `MimironSQL.DbContextGenerator` — included under `analyzers/`

The library multi-targets `netstandard2.0` and `net10.0`.

## Usage

The primary entry point is `DbdParser`, which implements `IDbdParser` from Contracts:

```csharp
var parser = new DbdParser();

// Parse from a file path
IDbdFile dbd = parser.Parse(@"C:\defs\Achievement.dbd");

// Parse from a stream
using var stream = File.OpenRead(@"C:\defs\Achievement.dbd");
IDbdFile dbd = parser.Parse(stream);
```

The returned `IDbdFile` exposes:

- `ColumnsByName` — column metadata from the `COLUMNS` section, keyed by name.
- `Layouts` — layout sections, each grouping layout hashes with their build blocks.
- `GlobalBuilds` — build blocks that appear without a preceding `LAYOUT` line.

## Key Types

| Type | Purpose |
|------|---------|
| `DbdParser` | Entry point — implements `IDbdParser`; delegates to `DbdFile.Parse` |
| `DbdFile` | Root parsed object — columns, layouts, and global builds |
| `DbdLayout` | A `LAYOUT` section — groups one or more layout hashes with their build blocks |
| `DbdBuildBlock` | A `BUILD` block — ordered list of field entries for a specific build range |
| `DbdLayoutEntry` | Single field within a build block — name, type, array count, inline flag, ID/relation markers |
| `DbdColumn` | Column-level metadata from the `COLUMNS` section — value type, foreign key reference, verified flag |
| `DbdColumnParser` | Parses a `COLUMNS` section line into a column name and `DbdColumn` |
| `DbdLayoutEntryParser` | Parses a `BUILD` section line into a `DbdLayoutEntry` |

## DBD File Format

A `.dbd` file has three main section types that this parser handles:

### COLUMNS

Declared at the top of the file. Each line specifies a column's type and name, with an optional foreign-key reference and a `?` suffix for unverified columns:

```
int ID
int<AchievementCategory::ID> Category
string Title_lang?
```

### LAYOUT

A `LAYOUT` line declares one or more comma-separated layout hashes (as 8-character hex values). Field entries that follow belong to this layout:

```
LAYOUT 1F4B39D2, A7E4804C
```

### BUILD

A `BUILD` line declares a version or version range. The field entries that follow describe the physical column order for matching client builds:

```
BUILD 11.0.7.58238-11.0.7.58824
$id$ID<32>
Category<16>
Title_lang
```

Field annotations include bit-width suffixes (`<32>`), array counts (`[4]`), the `$id$` marker for record IDs, `$relation$` for relation columns, and `$noninline$` for non-inline fields.

## Architecture

- Implements interfaces from `MimironSQL.Contracts` (`IDbdFile`, `IDbdLayout`, `IDbdBuildBlock`, `IDbdLayoutEntry`, `IDbdColumn`, `IDbdParser`).
- `DbdFile.Parse(Stream)` contains the core parsing state machine; `DbdParser` is the public-facing wrapper.
- Consumed by `MimironSQL.EntityFrameworkCore` for runtime schema resolution and by `MimironSQL.DbContextGenerator` for compile-time source generation.
- Embedded into downstream packages rather than referenced as a package dependency, so it does not appear in consumers' dependency graphs.

## License

[MIT](../../LICENSE.txt)
