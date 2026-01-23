using MimironSQL.Db2.Wdc5;

namespace MimironSQL.Db2.Query;

internal static class Db2DenseStringMatch
{
    public static bool Contains(Wdc5Row row, Db2FieldAccessor accessor, HashSet<int> matchingStarts)
        => Match(row, accessor, matchingStarts);

    public static bool StartsWith(Wdc5Row row, Db2FieldAccessor accessor, HashSet<int> matchingStarts)
        => Match(row, accessor, matchingStarts);

    public static bool EndsWith(Wdc5Row row, Db2FieldAccessor accessor, HashSet<int> matchingStarts)
        => Match(row, accessor, matchingStarts);

    private static bool Match(Wdc5Row row, Db2FieldAccessor accessor, HashSet<int> matchingStarts)
    {
        var fieldIndex = accessor.Field.ColumnStartIndex;
        if (fieldIndex < 0)
            return false;

        return row.TryGetDenseStringTableIndex(fieldIndex, out var stringTableIndex) && matchingStarts.Contains(stringTableIndex);
    }
}
