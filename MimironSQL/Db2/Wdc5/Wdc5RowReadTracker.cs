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
        public void Dispose() => _state.Value = null;
    }

    private static readonly AsyncLocal<State?> _state = new();

    public static IDisposable Start(int fieldsCount)
    {
        _state.Value = new State(fieldsCount);
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
        var state = _state.Value;
        if (state is not null)
            state.ScalarReads[fieldIndex]++;
    }

    internal static void OnArray(int fieldIndex)
    {
        var state = _state.Value;
        if (state is not null)
            state.ArrayReads[fieldIndex]++;
    }

    internal static void OnString(int fieldIndex)
    {
        var state = _state.Value;
        if (state is not null)
            state.StringReads[fieldIndex]++;
    }
}
