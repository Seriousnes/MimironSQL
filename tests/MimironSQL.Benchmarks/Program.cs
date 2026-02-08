using BenchmarkDotNet.Running;

using MimironSQL.Benchmarks;

if (!BenchmarkTestDataPaths.TryGetTestDataDirectory(out _))
{
	Console.Error.WriteLine("MimironSQL benchmark test data not found (tests/MimironSQL.IntegrationTests/TestData). Benchmarks skipped.");
	return;
}

BenchmarkSwitcher
	.FromAssembly(typeof(Program).Assembly)
	.Run(args);
