# Project Overview
This project is a SQL engine for reading and querying World of Warcraft DB2 files. It is implemented in C# and designed to be extensible for future file formats used by World of Warcraft. It will support reading DB2 files via the FileSystem (i.e. FileSystemDBDProvider) and via CASC (i.e. CascDBDProvider).

## IMPORTANT
- When you receive answers to questions as per the "Follow-up Questions" instructions, incorporate those answers into this project overview and the implementation plan as needed.
- Only consider files within the root directory of MimironSQL and its subdirectories as part of this project. Do not consider files from other repositories (such as DBCD, WoWDBDefs, or CASC.Net) except for understanding how to interface with them.
- Use minimal commenting style, only adding comments on methods or complex logic where absolutely necessary for clarity.

## Terminal (Hard)
- Assume all terminal commands run in **Windows PowerShell (pwsh)**, not `cmd.exe`.
- Do **not** use `cmd.exe`-specific syntax such as `cd /d`.
- Prefer `Set-Location` (or `cd`) with **absolute paths** (e.g., `Set-Location "g:\source\MimironSQL"`).
- Commands can be chained with `;` or `&&`.

## Modern C# / Style Rules (Hard)
- Target .NET 10 / C# 14 and prefer the newest language features.
- Follow the additional coding style rules in [coding-style.md](./instructions/coding-style.md).

## Performance 
- Follow the performance guidelines in [performance.md](./instructions/performance.md)

### Attributes (Hard)
- Do NOT add `[MethodImpl(...)]` attributes (including `MethodImplOptions.AggressiveInlining`).
- Remove any `[MethodImpl(...)]` attributes you encounter.
- Only add these attributes back after benchmarking demonstrates a real benefit.

## Repository Structure
- `MimironSQL/`: Main library project with the SQL engine implementation
- `MimironSQL.Formats.Abstractions/`: Abstract interfaces and base types for DB2 format support
- `MimironSQL.Formats.Wdc5/`: WDC5 format implementation (World of Warcraft DB2 version 5)
- `MimironSQL.Tests/`: Unit and integration tests using xUnit, NSubstitute, and Shouldly
- `Salsa20/`: Salsa20 encryption library used for encrypted DB2 files

## Development Workflow

### Building the Project
- Build: `dotnet build MimironSQL.slnx`
- This will restore NuGet packages and compile all projects in the solution

### Running Tests
- Run all tests: `dotnet test MimironSQL.slnx`
- Run tests without rebuilding: `dotnet test MimironSQL.slnx --no-build`
- All tests must pass before committing changes

## Nuget Packages
- Install packages using the dotnet CLI. Never edit the .csproj files directly.
- Prefer using latest stable versions of packages unless a specific version is required for compatibility.
- Prefer well known packages with active maintenance and good community support.

## Test Strategy
See [test-strategy.md](./instructions/test-strategy.md) for detailed testing instructions.

## File Specifications

### DB2 file structure
The DB2 file structure is documentation is available here - https://wowdev.wiki/DB2, this project is initially targetting WDC5 and later.

### Metadata
Since DB2 files don't contain any metadata about column names or data types, we're using the WoWDBDefs repository, including the C# implementation for validation

## Implementation Plan
The implementation plan is described in detail in [implementation-plan.md](./instructions/implementation-plan.md)

- Supporting docs:
    - DB2/WDC5 format notes: [db2-format.md](./instructions/db2-format.md)
    - Query engine notes: [query-engine-notes.md](./instructions/query-engine-notes.md)
    - Architecture overview: [architecture.md](./instructions/architecture.md)

- When receiving clarifications per the follow-up questions instruction, update the implementation plan and/or supporting docs as needed
- Use a git branch per phase, e.g. `feature/phase-1-virtual-table`, `feature/phase-2-schema-mapper`, etc.
    - Once a phase is complete, and all tests pass, merge into `main` before starting the next phase