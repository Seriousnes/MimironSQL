using System.Threading;

namespace MimironSQL.Db2.Wdc5;

internal readonly record struct Wdc5RowReadSnapshot(int[] ScalarReads, int[] ArrayReads, int[] StringReads);

internal static class Wdc5RowReadTracker
{
    private sealed class State(int fieldsCount)
    {
        public int[] ScalarReads { get; } = new int[fieldsCount];

        public int[] ArrayReads { get; } = new int[fieldsCount];

        public int[] StringReads { get; } = new int[fieldsCount];
    }

    private sealed class Scope : IDisposable
    {
        private int _disposed;

        public void Dispose()
        {
            if (Interlocked.Exchange(ref _disposed, 1) == 0)
            {
                try
                {
                    _state.Value = null;
                }
                finally
                {
                    Interlocked.Decrement(ref _activeScopes);
                }
            }
        }
    }

    private static readonly AsyncLocal<State?> _state = new();
    private static int _activeScopes;

    public static bool IsEnabled => Volatile.Read(ref _activeScopes) > 0;

    public static IDisposable Start(int fieldsCount)
    {
        var state = new State(fieldsCount);
        _state.Value = state;
        Interlocked.Increment(ref _activeScopes);
        return new Scope();
    }

    public static Wdc5RowReadSnapshot Snapshot()
    {
        var state = _state.Value;
        return state is null
            ? new Wdc5RowReadSnapshot([], [], [])
            : new Wdc5RowReadSnapshot([.. state.ScalarReads], [.. state.ArrayReads], [.. state.StringReads]);
    }

    internal static void OnScalar(int fieldIndex)
    {
        if (!IsEnabled)
            return;

        var state = _state.Value;
        if (state is not null)
            state.ScalarReads[fieldIndex]++;
    }

    internal static void OnArray(int fieldIndex)
    {
        if (!IsEnabled)
            return;

        var state = _state.Value;
        if (state is not null)
            state.ArrayReads[fieldIndex]++;
    }

    internal static void OnString(int fieldIndex)
    {
        if (!IsEnabled)
            return;

        var state = _state.Value;
        if (state is not null)
            state.StringReads[fieldIndex]++;
    }
}
