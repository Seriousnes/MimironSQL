using System.Globalization;

namespace MimironSQL.Dbd;

public sealed class DbdLayout(uint[] hashes)
{
    public uint[] Hashes { get; } = hashes;
    public List<DbdBuildBlock> Builds { get; } = [];

    public bool ContainsHash(uint hash)
    {
        foreach (var h in Hashes)
        {
            if (h == hash)
                return true;
        }

        return false;
    }

    public static DbdLayout ParseHeader(string line)
    {
        // "LAYOUT 2273DFFF, 60BB6C3F"
        var rest = line["LAYOUT ".Length..].Trim();
        var parts = rest.Split(',', StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries);
        var hashes = new uint[parts.Length];
        for (var i = 0; i < parts.Length; i++)
            hashes[i] = uint.Parse(parts[i], NumberStyles.HexNumber, CultureInfo.InvariantCulture);

        return new DbdLayout(hashes);
    }

    public bool TrySelectBuildByPhysicalColumnCount(int expected, out DbdBuildBlock build, out int[] availableCounts)
    {
        var counts = new int[Builds.Count];
        for (var i = 0; i < Builds.Count; i++)
        {
            var candidate = Builds[i];
            var count = candidate.GetPhysicalColumnCount();
            counts[i] = count;
            if (count == expected)
            {
                build = candidate;
                availableCounts = counts;
                return true;
            }
        }

        build = default!;
        availableCounts = counts;
        return false;
    }
}
