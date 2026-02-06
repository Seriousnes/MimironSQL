namespace MimironSQL.Dbd;

public sealed class DbdBuildBlock(string buildLine) : IDbdBuildBlock
{
    public string BuildLine { get; } = buildLine;
    public List<DbdLayoutEntry> Entries { get; } = [];

    IReadOnlyList<IDbdLayoutEntry> IDbdBuildBlock.Entries => Entries;

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
