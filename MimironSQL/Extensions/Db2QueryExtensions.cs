namespace MimironSQL.Extensions;

internal static class Db2QueryExtensions
{
    public static bool MatchesSharedPrimaryKeyNullCheck(this HashSet<int> existingIds, int id, bool isNotNull)
    {
        if (isNotNull)
            return id != 0 && existingIds.Contains(id);

        return id == 0 || !existingIds.Contains(id);
    }
}
