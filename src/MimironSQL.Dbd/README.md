# MimironSQL.Dbd

Parser for WoWDBDefs `.dbd` definition files. Produces the typed model (`DbdFile`, `DbdLayout`, `DbdBuildBlock`, `DbdLayoutEntry`, `DbdColumn`) that implements the interfaces defined in `MimironSQL.Contracts`.

This is an internal library — not published as a standalone package. It is referenced by `MimironSQL.EntityFrameworkCore` and `MimironSQL.DbContextGenerator`.

The `MimironSQL.Dbd` implementation assembly is shipped as an embedded dependency (e.g. included under `lib/...` for the EF Core provider and under `analyzers/...` for the source generator).

## Key Types

| Type | Purpose |
|------|---------|
| `DbdFile` | Root object — parse a `.dbd` stream via `DbdFile.Parse(stream)` |
| `DbdLayout` | Groups layout hashes and their associated build blocks |
| `DbdBuildBlock` | Ordered field list for a specific build range |
| `DbdLayoutEntry` | Single field — name, type, array count, inline flag, ID/relation markers |
| `DbdColumn` | Column-level metadata — value type, foreign key reference |
| `DbdColumnParser` | Parses a `COLUMNS` section line into a name + `DbdColumn` |
| `DbdLayoutEntryParser` | Parses a `BUILD` section line into a `DbdLayoutEntry` |
