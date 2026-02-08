# MimironSQL.Benchmarks

BenchmarkDotNet performance suite.

## Usage

```bash
cd src/MimironSQL.Benchmarks
dotnet run -c Release
```

## Filters

```bash
dotnet run -c Release --filter *QueryBenchmarks*
```

## Export

```bash
dotnet run -c Release --exporters json,csv
```

Target: .NET 10.0
