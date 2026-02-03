using MimironSQL.Dbd;

namespace MimironSQL.Providers;

public sealed class FileSystemDbdProvider(FileSystemDbdProviderOptions options) : IDbdProvider
{
    public IDbdFile Open(string tableName) => DbdFile.Parse(File.OpenRead(Path.Combine(options.DefinitionsDirectory, $"{tableName}.dbd")));
}
