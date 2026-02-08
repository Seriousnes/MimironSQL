# MimironSQL.Benchmarks

Performance benchmarking suite for MimironSQL using BenchmarkDotNet. Measures and tracks performance characteristics of the query engine, format readers, and providers.

## Overview

`MimironSQL.Benchmarks` is a standalone console application that runs performance benchmarks using BenchmarkDotNet. It helps:

- **Measure Performance**: Quantify query execution time, memory usage, and allocations
- **Track Regressions**: Detect performance degradation across changes
- **Guide Optimization**: Identify bottlenecks and optimization opportunities
- **Compare Approaches**: Evaluate different implementation strategies

## Package Information

- **Package ID**: N/A (not packaged - development tool)
- **Target Framework**: .NET 10.0
- **Type**: Console Application (Executable)
- **Dependencies**:
  - `BenchmarkDotNet` 0.15.8
  - `MimironSQL.EntityFrameworkCore`
  - `MimironSQL.Providers.FileSystem`

## Running Benchmarks

### Quick Start

```bash
cd src/MimironSQL.Benchmarks
dotnet run -c Release
```

**Important:** Always run benchmarks in Release mode for accurate results.

### With Filters

Run specific benchmark classes or methods:

```bash
# Run all benchmarks in a class
dotnet run -c Release --filter *QueryBenchmarks*

# Run specific benchmark method
dotnet run -c Release --filter *QueryBenchmarks.SimpleWhere*

# Run benchmarks matching pattern
dotnet run -c Release --filter *FileReading*
```

### Export Results

```bash
# Export to CSV
dotnet run -c Release --exporters csv

# Export to JSON
dotnet run -c Release --exporters json

# Multiple exporters
dotnet run -c Release --exporters html,csv,json
```

## Benchmark Categories

### Query Performance Benchmarks

Measure EF Core query execution performance:

```csharp
[MemoryDiagnoser]
public class QueryBenchmarks
{
    [Benchmark]
    public void SimpleWhere()
    {
        var results = context.Map
            .Where(m => m.Id < 100)
            .ToList();
    }
    
    [Benchmark]
    public void WithInclude()
    {
        var results = context.MapChallengeMode
            .Include(mc => mc.Map)
            .Take(50)
            .ToList();
    }
    
    [Benchmark]
    public void ComplexQuery()
    {
        var results = context.Spell
            .Where(s => s.Id > 1000 && s.Id < 2000)
            .Include(s => s.SpellName)
            .Select(s => new { s.Id, s.SpellName.Name })
            .ToList();
    }
}
```

### Format Reading Benchmarks

Measure DB2 file reading and parsing:

```csharp
[MemoryDiagnoser]
public class FormatReadingBenchmarks
{
    [Benchmark]
    public void OpenFile()
    {
        using var stream = File.OpenRead("Map.db2");
        using var file = format.OpenFile(stream);
    }
    
    [Benchmark]
    public void ReadAllRows()
    {
        using var file = format.OpenFile(stream);
        for (int i = 0; i < file.RecordCount; i++)
        {
            var row = file.GetRow(i);
            _ = row.Id;
        }
    }
    
    [Benchmark]
    public void ReadWithDecryption()
    {
        // Benchmarks reading encrypted files
        using var file = format.OpenFile(encryptedStream);
        // ...
    }
}
```

### Provider Benchmarks

Measure provider performance:

```csharp
[MemoryDiagnoser]
public class ProviderBenchmarks
{
    [Benchmark]
    public void OpenDb2Stream()
    {
        using var stream = db2Provider.OpenDb2Stream("Map");
    }
    
    [Benchmark]
    public void OpenDbdFile()
    {
        var dbdFile = dbdProvider.Open("Map");
    }
    
    [Benchmark]
    public void TactKeyLookup()
    {
        tactKeyProvider.TryGetKey(keyName, out var key);
    }
}
```

### Memory Allocation Benchmarks

Track allocation patterns:

```csharp
[MemoryDiagnoser]
[SimpleJob(RuntimeMoniker.Net100)]
public class AllocationBenchmarks
{
    [Benchmark]
    public void QueryWithNoTracking()
    {
        var results = context.Map
            .AsNoTracking()
            .Take(100)
            .ToList();
    }
    
    [Benchmark]
    public void QueryWithTracking()
    {
        var results = context.Map
            .Take(100)
            .ToList();
    }
}
```

## Benchmark Configuration

### Global Configuration

```csharp
[Config(typeof(BenchmarkConfig))]
public class MyBenchmarks
{
    // Benchmarks...
}

public class BenchmarkConfig : ManualConfig
{
    public BenchmarkConfig()
    {
        AddDiagnoser(MemoryDiagnoser.Default);
        AddJob(Job.Default.WithRuntime(CoreRuntime.Core100));
        AddExporter(MarkdownExporter.GitHub);
        AddLogger(ConsoleLogger.Default);
    }
}
```

### Benchmark Attributes

```csharp
[MemoryDiagnoser]           // Track allocations
[SimpleJob]                 // Use default job
[DryJob]                    // Quick test run
[RankColumn]                // Add rank column
[Orderer(SummaryOrderPolicy.FastestToSlowest)]  // Sort results
public class MyBenchmarks
{
    // ...
}
```

### Parameters

Run benchmarks with different parameters:

```csharp
[Params(10, 100, 1000)]
public int RecordCount { get; set; }

[Benchmark]
public void QueryWithLimit()
{
    var results = context.Map
        .Take(RecordCount)
        .ToList();
}
```

## Understanding Results

### Benchmark Output

```
| Method           | Mean      | Error    | StdDev   | Gen0   | Gen1  | Allocated |
|----------------- |----------:|--------- |---------:|-------:|------:|----------:|
| SimpleWhere      | 125.3 μs  | 2.1 μs   | 1.9 μs   | 15.625 | -     | 64.5 KB   |
| WithInclude      | 245.7 μs  | 4.3 μs   | 3.8 μs   | 31.250 | 1.953 | 128 KB    |
| ComplexQuery     | 387.2 μs  | 7.2 μs   | 6.4 μs   | 47.852 | 3.906 | 196 KB    |
```

**Columns:**
- **Mean**: Average execution time
- **Error**: Standard error of the mean
- **StdDev**: Standard deviation
- **Gen0/Gen1**: Garbage collection counts
- **Allocated**: Total memory allocated

### Performance Analysis

**Good Performance:**
- Low Mean time (< 1ms for simple queries)
- Low StdDev (consistent results)
- Minimal allocations
- Few Gen1/Gen2 collections

**Performance Issues:**
- High Mean time (> 10ms for simple queries)
- High StdDev (inconsistent results)
- Excessive allocations (> 1MB)
- Frequent Gen2 collections

## Best Practices

### 1. Always Run in Release Mode

```bash
# ✅ Correct
dotnet run -c Release

# ❌ Wrong - gives misleading results
dotnet run -c Debug
```

### 2. Warm Up Before Benchmarking

```csharp
[GlobalSetup]
public void Setup()
{
    // Initialize context
    context = CreateContext();
    
    // Warm up - run queries once to JIT compile
    context.Map.FirstOrDefault();
}
```

### 3. Use Realistic Data

```csharp
// ✅ Use actual DB2 files
var options = new FileSystemDb2StreamProviderOptions
{
    Db2DirectoryPath = @"C:\WoW\DBFilesClient"
};

// ❌ Don't use tiny test files
```

### 4. Isolate What You're Testing

```csharp
// ✅ Good - tests only query execution
[Benchmark]
public void QueryOnly()
{
    var results = context.Map.Take(100).ToList();
}

// ❌ Bad - includes file I/O
[Benchmark]
public void QueryWithFileOpen()
{
    using var context = CreateNewContext();  // Opens files
    var results = context.Map.Take(100).ToList();
}
```

### 5. Compare Fairly

```csharp
// Compare similar operations
[Benchmark(Baseline = true)]
public void BaselineQuery() { /* ... */ }

[Benchmark]
public void OptimizedQuery() { /* ... */ }
```

## Common Scenarios

### Comparing Different Approaches

```csharp
[MemoryDiagnoser]
public class ApproachComparison
{
    [Benchmark(Baseline = true)]
    public void CurrentApproach()
    {
        // Current implementation
    }
    
    [Benchmark]
    public void AlternativeApproach()
    {
        // Alternative implementation to compare
    }
}
```

### Testing Scalability

```csharp
[Params(10, 100, 1000, 10000)]
public int N { get; set; }

[Benchmark]
public void ScalabilityTest()
{
    var results = context.Map.Take(N).ToList();
}
```

### Memory Profiling

```csharp
[MemoryDiagnoser]
[SimpleJob(invocationCount: 1, warmupCount: 0)]
public class MemoryProfile
{
    [Benchmark]
    public void MemoryIntensiveOperation()
    {
        // Operation to profile
    }
}
```

## Integration with CI/CD

### Automated Performance Testing

```yaml
# Example GitHub Actions workflow
- name: Run Benchmarks
  run: |
    cd src/MimironSQL.Benchmarks
    dotnet run -c Release --exporters json
    
- name: Upload Results
  uses: actions/upload-artifact@v2
  with:
    name: benchmark-results
    path: BenchmarkDotNet.Artifacts/results/*.json
```

### Regression Detection

Compare results against baseline:

```bash
# Run and save baseline
dotnet run -c Release -- --job short --memory
cp BenchmarkDotNet.Artifacts/results/* baseline/

# After changes, compare
dotnet run -c Release -- --job short --memory
# Compare with baseline/
```

## Performance Goals

### Target Metrics

**Query Performance:**
- Simple queries: < 1ms
- Queries with Include: < 5ms
- Complex queries: < 10ms

**Memory:**
- Minimal boxing/unboxing
- < 100KB allocations per query

**Throughput:**
- > 10,000 rows/second for simple reads
- > 1,000 rows/second for complex queries

## Troubleshooting

### Benchmark Not Running

Check prerequisites:
```bash
# Verify project builds
dotnet build -c Release

# Check BenchmarkDotNet is installed
dotnet list package | grep BenchmarkDotNet
```

### Inconsistent Results

Reduce noise:
1. Close other applications
2. Disable CPU throttling
3. Use longer warmup/iteration counts
4. Run multiple times and average

### Out of Memory

Reduce data size or use pagination:
```csharp
[Params(100, 1000)]  // Not 10000
public int RecordCount { get; set; }
```

## Tools and Visualization

### BenchmarkDotNet Diagnosers

```csharp
[Config(typeof(Config))]
public class MyBenchmarks
{
    private class Config : ManualConfig
    {
        public Config()
        {
            AddDiagnoser(MemoryDiagnoser.Default);
            AddDiagnoser(ThreadingDiagnoser.Default);
            AddDiagnoser(new EventPipeProfiler(EventPipeProfile.CpuSampling));
        }
    }
}
```

### External Tools

- **dotTrace**: JetBrains profiler
- **PerfView**: Microsoft performance analyzer
- **Visual Studio Profiler**: Built-in profiling

## Example Benchmark Run

```bash
$ cd src/MimironSQL.Benchmarks
$ dotnet run -c Release

// * Summary *

BenchmarkDotNet v0.15.8, Windows 11
Intel Core i9-12900K, 1 CPU, 24 logical and 16 physical cores
.NET SDK 10.0.0
  [Host]     : .NET 10.0.0, X64 RyuJIT AVX2
  DefaultJob : .NET 10.0.0, X64 RyuJIT AVX2

| Method           | Mean      | Error    | StdDev   | Allocated |
|----------------- |----------:|---------:|---------:|----------:|
| SimpleQuery      |  87.23 μs | 1.234 μs | 1.154 μs |  42.18 KB |
| QueryWithInclude | 156.78 μs | 2.345 μs | 2.193 μs |  89.34 KB |
```

## Contributing Benchmarks

When adding new benchmarks:

1. Focus on representative scenarios
2. Include memory diagnostics
3. Document what's being measured
4. Add baseline comparisons when useful
5. Keep benchmarks fast (< 1 second per iteration)

## Related Packages

- **BenchmarkDotNet**: Benchmarking framework
- **MimironSQL.EntityFrameworkCore**: Benchmarked component
- **MimironSQL.Formats.Wdc5**: Benchmarked component
- **MimironSQL.Providers.FileSystem**: Benchmarked component

## See Also

- [Root README](../../README.md)
- [Performance Guidelines](../../.github/instructions/performance.md)
- [BenchmarkDotNet Documentation](https://benchmarkdotnet.org/)
