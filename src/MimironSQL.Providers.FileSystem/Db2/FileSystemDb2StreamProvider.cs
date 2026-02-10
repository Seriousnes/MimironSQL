namespace MimironSQL.Providers;

/// <summary>
/// File-system implementation of <see cref="IDb2StreamProvider"/>.
/// </summary>
/// <param name="options">Options for locating DB2 files on disk.</param>
public sealed class FileSystemDb2StreamProvider(FileSystemDb2StreamProviderOptions options) : IDb2StreamProvider
{
    private readonly Dictionary<string, string> _pathsByTableName = Directory
        .EnumerateFiles(options.Db2DirectoryPath, "*.db2", SearchOption.TopDirectoryOnly)
        .ToDictionary(
            p => Path.GetFileNameWithoutExtension(p),
            p => p,
            StringComparer.OrdinalIgnoreCase);

    /// <inheritdoc />
    public Stream OpenDb2Stream(string tableName)
    {
        ArgumentNullException.ThrowIfNull(tableName);

        if (_pathsByTableName.TryGetValue(tableName, out var path))
            return File.OpenRead(path);

        throw new FileNotFoundException($"No .db2 file found for table '{tableName}' in '{options.Db2DirectoryPath}'.");
    }

    /// <inheritdoc />
    public Task<Stream> OpenDb2StreamAsync(string tableName, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(tableName);
        if (_pathsByTableName.TryGetValue(tableName, out var path))
        {
            return Task.FromResult<Stream>(File.OpenRead(path));
        }
        throw new FileNotFoundException($"No .db2 file found for table '{tableName}' in '{options.Db2DirectoryPath}'.");
    }
}
