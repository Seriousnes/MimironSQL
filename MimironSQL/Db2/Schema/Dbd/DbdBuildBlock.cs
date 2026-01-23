using System.Collections.Generic;

namespace MimironSQL.Db2.Schema.Dbd;

internal sealed class DbdBuildBlock(string buildLine)
{
    public string BuildLine { get; } = buildLine;
    public List<DbdLayoutEntry> Entries { get; } = [];

    public int GetPhysicalColumnCount()
    {
        var count = 0;
        foreach (var entry in Entries)
        {
            if (entry.IsNonInline)
                continue;

            count++;
        }

        return count;
    }
}
