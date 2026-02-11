using BenchmarkDotNet.Running;

using MimironSQL.Benchmarks;

if (!BenchmarkTestDataPaths.TryGetTestDataDirectory(out var path))
{
    Console.Error.WriteLine($"MimironSQL benchmark test data not found in {Path.GetFullPath(path)} (tests/MimironSQL.IntegrationTests/TestData). Benchmarks skipped.");
    return;
}

BenchmarkSwitcher
    .FromAssembly(typeof(Program).Assembly)
    .Run(args);
