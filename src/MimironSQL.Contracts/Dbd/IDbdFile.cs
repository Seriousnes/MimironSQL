namespace MimironSQL.Dbd;

public interface IDbdFile
{
    IReadOnlyDictionary<string, IDbdColumn> ColumnsByName { get; }

    IReadOnlyList<IDbdLayout> Layouts { get; }

    IReadOnlyList<IDbdBuildBlock> GlobalBuilds { get; }

    bool TryGetLayout(uint layoutHash, out IDbdLayout layout);
}
