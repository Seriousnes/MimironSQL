# Project Guidelines

## Code Style
- Target .NET 10 / C# 14 and prefer modern language features unless compatibility requires otherwise.
- Follow detailed style guidance in [coding-style.md](./instructions/coding-style.md).
- Do not add `[MethodImpl(...)]` attributes (including `MethodImplOptions.AggressiveInlining`). Remove them when encountered unless benchmarking proves benefit.
- Prefer `[InternalsVisibleTo]` for test/benchmark access instead of widening internal types to public.

## Architecture
- MimironSQL is a read-only EF Core provider for World of Warcraft DB2 files, with schema sourced from WoWDBDefs `.dbd` definitions.
- Core boundaries:
    - `src/MimironSQL.Contracts`: extension interfaces and shared contracts
    - `src/MimironSQL.Dbd`: `.dbd` parser and model
    - `src/MimironSQL.DbContextGenerator`: source-generated entities and `WoWDb2Context`
    - `src/MimironSQL.EntityFrameworkCore`: provider pipeline
    - `src/MimironSQL.Formats.Wdc5`: WDC5 reader
    - `src/MimironSQL.Providers.FileSystem` and `src/MimironSQL.Providers.CASC`: data access providers
    - `src/Salsa20`: encrypted section support
- Follow the EF Core provider bootstrap plan in [wip-iterative-bootstrap.md](./instructions/wip-iterative-bootstrap.md).

## Build and Test
- Assume terminal commands run in Windows PowerShell (`pwsh`), not `cmd.exe`.
- Build: `dotnet build MimironSQL.slnx`
- Test: `dotnet test MimironSQL.slnx`
- Test (no rebuild): `dotnet test MimironSQL.slnx --no-build`
- Coverage helper: see [tools/coverage/README.md](../tools/coverage/README.md)
- If tests are skipped due to build failure, fix build errors first.
- All local test suites should pass before committing.

## Conventions
- Install packages with the dotnet CLI; do not hand-edit `.csproj` package references.
- Keep instruction content concise and link to detail docs instead of duplicating:
    - [test-strategy.md](./instructions/test-strategy.md)
    - [performance.md](./instructions/performance.md)
    - [db2-format.md](./instructions/db2-format.md)
- DB2 files have no embedded schema metadata; use WoWDBDefs `.dbd` definitions for names/types and generator input.
- DB2 format reference: https://wowdev.wiki/DB2