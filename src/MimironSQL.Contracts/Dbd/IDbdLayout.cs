namespace MimironSQL.Dbd;

public interface IDbdLayout
{
    IReadOnlyList<uint> Hashes { get; }

    IReadOnlyList<IDbdBuildBlock> Builds { get; }

    bool ContainsHash(uint hash);

    bool TrySelectBuildByPhysicalColumnCount(int expected, out IDbdBuildBlock build, out int[] availableCounts);
}
