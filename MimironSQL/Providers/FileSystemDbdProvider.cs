namespace MimironSQL.Providers;

public sealed class FileSystemDbdProvider(FileSystemDbdProviderOptions options) : IDbdProvider
{
    public Stream Open(string tableName)
        => File.OpenRead(Path.Combine(options.DefinitionsDirectory, $"{tableName}.dbd"));
}
