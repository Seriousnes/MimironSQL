namespace MimironSQL.Dbd;

public interface IDbdBuildBlock
{
    string BuildLine { get; }

    IReadOnlyList<IDbdLayoutEntry> Entries { get; }

    int GetPhysicalColumnCount();
}
