namespace MimironSQL.Formats.Wdc5.Db2;

/// <summary>
/// Snapshot of WDC5 file lookup counters.
/// </summary>
/// <param name="TotalTryGetRowByIdCalls">Total calls to row ID lookups within the current tracking scope.</param>
public readonly record struct Wdc5FileLookupSnapshot(int TotalTryGetRowByIdCalls);

/// <summary>
/// Provides lightweight, async-local counters for diagnosing WDC5 lookup behavior.
/// </summary>
public static class Wdc5FileLookupTracker
{
    private sealed class State
    {
        public int TotalTryGetRowByIdCalls;
    }

    private sealed class Scope : IDisposable
    {
        public void Dispose() => _state.Value = null;
    }

    private static readonly AsyncLocal<State?> _state = new();

    /// <summary>
    /// Starts tracking lookup counters for the current async flow.
    /// </summary>
    /// <returns>A scope that stops tracking when disposed.</returns>
    public static IDisposable Start()
    {
        _state.Value = new State();
        return new Scope();
    }

    /// <summary>
    /// Gets a snapshot of the current counters.
    /// </summary>
    /// <returns>The current snapshot.</returns>
    public static Wdc5FileLookupSnapshot Snapshot()
        => new(_state.Value?.TotalTryGetRowByIdCalls ?? 0);

    internal static void OnTryGetRowById()
    {
        var state = _state.Value;
        if (state is not null)
            state.TotalTryGetRowByIdCalls++;
    }
}
