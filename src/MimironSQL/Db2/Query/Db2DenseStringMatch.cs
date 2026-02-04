using MimironSQL.Formats;

namespace MimironSQL.Db2.Query;

internal static class Db2DenseStringMatch
{
    public static bool Contains<TRow>(IDb2DenseStringTableIndexProvider<TRow> provider, TRow row, int fieldIndex, HashSet<int> matchingStarts)
        where TRow : struct
        => Match(provider, row, fieldIndex, matchingStarts);

    public static bool StartsWith<TRow>(IDb2DenseStringTableIndexProvider<TRow> provider, TRow row, int fieldIndex, HashSet<int> matchingStarts)
        where TRow : struct
        => Match(provider, row, fieldIndex, matchingStarts);

    public static bool EndsWith<TRow>(IDb2DenseStringTableIndexProvider<TRow> provider, TRow row, int fieldIndex, HashSet<int> matchingStarts)
        where TRow : struct
        => Match(provider, row, fieldIndex, matchingStarts);

    private static bool Match<TRow>(IDb2DenseStringTableIndexProvider<TRow> provider, TRow row, int fieldIndex, HashSet<int> matchingStarts)
        where TRow : struct
    {
        return fieldIndex switch
        {
            < 0 => false,
            _ => provider.TryGetDenseStringTableIndex(row, fieldIndex, out var stringTableIndex) && matchingStarts.Contains(stringTableIndex),
        };
    }
}
