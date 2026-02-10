using MimironSQL.Dbd;

namespace MimironSQL.Providers;

/// <summary>
/// File-system implementation of <see cref="IDbdProvider"/>.
/// </summary>
/// <param name="options">Options for locating DBD files on disk.</param>
/// <param name="dbdParser">Parser used to read and parse DBD files.</param>
public sealed class FileSystemDbdProvider(FileSystemDbdProviderOptions options, IDbdParser dbdParser) : IDbdProvider
{
    private readonly IDbdParser _dbdParser = dbdParser ?? throw new ArgumentNullException(nameof(dbdParser));

    /// <inheritdoc />
    public IDbdFile Open(string tableName)
    {
        ArgumentNullException.ThrowIfNull(tableName);

        var path = Path.Combine(options.DefinitionsDirectory, $"{tableName}.dbd");
        return _dbdParser.Parse(path);
    }
}
