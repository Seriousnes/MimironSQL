# Project Overview
This project is a SQL engine for reading and querying World of Warcraft DB2 files. It is implemented in C# and designed to be extensible for future file formats used by World of Warcraft. It will support reading DB2 files via the FileSystem (i.e. FileSystemDBDProvider) and via CASC (i.e. CascDBDProvider).

## IMPORTANT
- Ignore all other `copilot-instructions.md` in this workspace.
- Only consider files within the root directory of MimironSQL and its subdirectories as part of this project. Do not consider files from other repositories (such as DBCD, WoWDBDefs, or CASC.Net) except for understanding how to interface with them.
- Never modifiy files outside of the MimironSQL repository.
- Use minimal commenting style, only adding comments on methods or complex logic where absolutely necessary for clarity.

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

- There is a progress log section and the end of this file. Keep it updated with progress through phases and steps
- When receiving clarifications per the follow-up questions instruction, update the implementation plan as needed
- Use a git branch per phase, e.g. `feature/phase-1-virtual-table`, `feature/phase-2-schema-mapper`, etc.
    - Once a phase is complete, and all tests pass, merge into `main` before starting the next phase