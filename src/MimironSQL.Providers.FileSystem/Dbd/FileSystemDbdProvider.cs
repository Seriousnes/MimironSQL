using MimironSQL.Dbd;

namespace MimironSQL.Providers;

public sealed class FileSystemDbdProvider(FileSystemDbdProviderOptions options, IDbdParser dbdParser) : IDbdProvider
{
    private readonly IDbdParser _dbdParser = dbdParser ?? throw new ArgumentNullException(nameof(dbdParser));

    public IDbdFile Open(string tableName)
    {
        ArgumentNullException.ThrowIfNull(tableName);

        var path = Path.Combine(options.DefinitionsDirectory, $"{tableName}.dbd");
        return _dbdParser.Parse(path);
    }
}
