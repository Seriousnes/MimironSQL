using MimironSQL.Providers;

namespace MimironSQL.Tests.Fixtures;

/// <summary>
/// Test provider that simulates failures for specific tables.
/// Used to test error handling in Include operations.
/// </summary>
internal class BrokenDb2StreamProvider : IDb2StreamProvider
{
    private readonly FileSystemDb2StreamProvider _innerProvider;
    private readonly Func<string, bool> _shouldFail;

    public BrokenDb2StreamProvider(string testDataDir, Func<string, bool> shouldFail)
    {
        _innerProvider = new FileSystemDb2StreamProvider(new FileSystemDb2StreamProviderOptions(testDataDir));
        _shouldFail = shouldFail;
    }

    public Stream OpenDb2Stream(string tableName)
    {
        if (_shouldFail(tableName))
            throw new InvalidOperationException($"SimulatedFailure: Cannot open table '{tableName}'");

        return _innerProvider.OpenDb2Stream(tableName);
    }
}
