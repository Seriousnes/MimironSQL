using System.Threading;

namespace MimironSQL.Db2.Wdc5;

internal readonly record struct Wdc5FileLookupSnapshot(int TotalTryGetRowByIdCalls);

internal static class Wdc5FileLookupTracker
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

    public static IDisposable Start()
    {
        _state.Value = new State();
        return new Scope();
    }

    public static Wdc5FileLookupSnapshot Snapshot()
        => new(_state.Value?.TotalTryGetRowByIdCalls ?? 0);

    internal static void OnTryGetRowById()
    {
        var state = _state.Value;
        if (state is not null)
            state.TotalTryGetRowByIdCalls++;
    }
}
