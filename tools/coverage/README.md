# Coverage helper: reportcoverage.cs

This folder contains a small helper script to **generate** a merged Cobertura report and **rank coverage misses** (with optional class/method drill-down).

It combines these steps into a single command:

1. Run tests with the Coverlet collector (`dotnet test ... --collect:"XPlat Code Coverage"`)
2. Merge per-test-project Cobertura outputs into a single `Cobertura.xml` (via ReportGenerator)
3. Print `Summary.txt`
4. Rank classes/files, or drill into a class/method

## Typical workflow (repo root)

Generate coverage + merged report + ranking:

```powershell
dotnet run .\tools\coverage\reportcoverage.cs -- --top 30 --files
```

For quicker iterations when you already have a recent merged report (no test run, no merge):

```powershell
dotnet run .\tools\coverage\reportcoverage.cs -- --skip-tests --top 30 --percent
```

## Usage

```text
dotnet run .\tools\coverage\reportcoverage.cs -- [--skip-tests] [--no-build] [--filter <substring>] [--top <N>] [--files] [--percent]
dotnet run .\tools\coverage\reportcoverage.cs -- [--skip-tests] [--no-build] --class <substring> [--methods] [--missed-lines] [--top <N>]
```

### Defaults

- Default mode runs: `dotnet test` + `reportgenerator` + analysis
- `--top`: `30`

### Modes

- (default): creates a new `coverage/history/<timestamp>/...` run, runs tests + merge, then analyzes the newly produced `merged/Cobertura.xml`.
- `--skip-tests`: analyzes the latest `coverage/history/*/merged/Cobertura.xml` (no test run, no merge).
- `--no-build`: passes `--no-build` through to `dotnet test` (default mode only).

### Options

- `--filter <substring>`: Only include classes whose class name or source filename contains the substring (case-insensitive).
- `--top <N>`: Show only the top N rows.
- `--files`: Also print an aggregate “top files by missed lines” table (grouped by Cobertura `filename`).
- `--percent`: Sort by coverage percentage (ascending) instead of missed lines.
- `--class <substring>`: Select a single Cobertura `<class>` by name substring (case-insensitive) and print details.
- `--methods`: With `--class`, print per-method missed line ranges for the selected class.
- `--missed-lines`: With `--class`, print missed line ranges for the selected class (from class-level `<lines>`).
- `--help` / `-h`: Print usage.

## Examples

### 1) Generate coverage + rank classes (default)

```powershell
dotnet run .\tools\coverage\reportcoverage.cs -- --top 30
```

### 2) Generate coverage without rebuilding

```powershell
dotnet run .\tools\coverage\reportcoverage.cs -- --no-build --top 30
```

### 3) Re-run reporting only (latest merged report)

```powershell
dotnet run .\tools\coverage\reportcoverage.cs -- --skip-tests --percent --top 30
```

### 4) Include a “top files” table as well

```powershell
dotnet run .\tools\coverage\reportcoverage.cs -- --top 25 --files
```

### 5) Focus the list to a subsystem (class or filename substring)

```powershell
dotnet run .\tools\coverage\reportcoverage.cs -- --filter Db2RowProjector --top 50
```

### 6) Drill into a specific class (and list missed line ranges)

```powershell
dotnet run .\tools\coverage\reportcoverage.cs -- --class MimironSQL.Db2.Query.Db2RowProjectorCompiler --missed-lines
```

### 7) Drill into a class and see which methods contribute misses

```powershell
dotnet run .\tools\coverage\reportcoverage.cs -- --class MimironSQL.Db2.Query.Db2RowProjectorCompiler --methods --top 20
```
