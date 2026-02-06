using System.Text;

namespace MimironSQL.Db2.Query;

internal enum Db2StringMatchKind
{
    Contains = 0,
    StartsWith,
    EndsWith,
}

internal static class Db2DenseStringScanner
{
    public static HashSet<int> FindStartOffsets(ReadOnlySpan<byte> bytes, string needle, Db2StringMatchKind kind)
    {
        if (string.IsNullOrEmpty(needle))
            return [];

        var needleBytes = Encoding.UTF8.GetBytes(needle);
        if (needleBytes is { Length: 0 })
            return [];

        HashSet<int> starts = [];

        var idx = 0;
        while (idx < bytes.Length)
        {
            var found = bytes[idx..].IndexOf(needleBytes);
            if (found < 0)
                break;

            var matchIndex = idx + found;

            var start = matchIndex;
            while (start > 0 && bytes[start - 1] != 0)
                start--;

            var add = kind switch
            {
                Db2StringMatchKind.Contains => true,
                Db2StringMatchKind.StartsWith => start == matchIndex,
                Db2StringMatchKind.EndsWith => matchIndex + needleBytes.Length < bytes.Length && bytes[matchIndex + needleBytes.Length] == 0,
                _ => false,
            };

            if (add)
                starts.Add(start);

            idx = matchIndex + 1;
        }

        return starts;
    }
}
