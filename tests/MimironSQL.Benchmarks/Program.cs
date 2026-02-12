using BenchmarkDotNet.Running;

using MimironSQL.Benchmarks;

if (!BenchmarkTestDataPaths.TryGetTestDataDirectory(out var path))
{
    Console.Error.WriteLine($"MimironSQL benchmark test data not found. Expected folder 'tests/TestData' somewhere above '{AppContext.BaseDirectory}'. Benchmarks skipped.");
    return;
}

BenchmarkSwitcher
    .FromAssembly(typeof(Program).Assembly)
    .Run(args);
