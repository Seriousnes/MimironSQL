using System.Globalization;

namespace MimironSQL.Dbd;

public sealed class DbdLayout(uint[] hashes) : IDbdLayout
{
    public uint[] Hashes { get; } = hashes;
    public List<DbdBuildBlock> Builds { get; } = [];

    IReadOnlyList<uint> IDbdLayout.Hashes => Hashes;
    IReadOnlyList<IDbdBuildBlock> IDbdLayout.Builds => Builds;

    public bool ContainsHash(uint hash)
        => Hashes.Contains(hash);

    public static DbdLayout ParseHeader(string line)
    {
        // "LAYOUT 2273DFFF, 60BB6C3F"
        var rest = line.AsSpan("LAYOUT ".Length).Trim();
        if (rest is { Length: 0 })
            return new DbdLayout([]);

        var count = 1;
        foreach (var c in rest)
        {
            if (c == ',')
                count++;
        }

        var hashes = new uint[count];
        var index = 0;
        while (rest.Length != 0)
        {
            var comma = rest.IndexOf(',');
            var token = comma >= 0 ? rest.Slice(0, comma) : rest;
            hashes[index++] = uint.Parse(token.Trim().ToString(), NumberStyles.HexNumber, CultureInfo.InvariantCulture);

            if (comma < 0)
                break;

            rest = rest.Slice(comma + 1);
        }

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

    bool IDbdLayout.TrySelectBuildByPhysicalColumnCount(int expected, out IDbdBuildBlock build, out int[] availableCounts)
    {
        if (TrySelectBuildByPhysicalColumnCount(expected, out var concreteBuild, out availableCounts))
        {
            build = concreteBuild;
            return true;
        }

        build = default!;
        return false;
    }
}
