using MimironSQL.Providers;

namespace MimironSQL.Tests.Fixtures;

/// <summary>
/// Test provider that simulates failures for specific tables.
/// Used to test error handling in Include operations.
/// </summary>
internal class BrokenDb2StreamProvider(string testDataDir, Func<string, bool> shouldFail) : IDb2StreamProvider
{
    private readonly FileSystemDb2StreamProvider _innerProvider = new(new FileSystemDb2StreamProviderOptions(testDataDir));
    private readonly Func<string, bool> _shouldFail = shouldFail;

    public Stream OpenDb2Stream(string tableName)
    {
        if (_shouldFail(tableName))
            throw new InvalidOperationException($"SimulatedFailure: Cannot open table '{tableName}'");

        return _innerProvider.OpenDb2Stream(tableName);
    }
}
