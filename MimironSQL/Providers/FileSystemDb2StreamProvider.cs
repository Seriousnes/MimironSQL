using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace MimironSQL.Providers;

public sealed class FileSystemDb2StreamProvider(FileSystemDb2StreamProviderOptions options) : IDb2StreamProvider
{
    private readonly IReadOnlyDictionary<string, string> _pathsByTableName = Directory
        .EnumerateFiles(options.Db2DirectoryPath, "*.db2", SearchOption.TopDirectoryOnly)
        .ToDictionary(
            p => Path.GetFileNameWithoutExtension(p),
            p => p,
            StringComparer.OrdinalIgnoreCase);

    public Stream OpenDb2Stream(string tableName)
    {
        ArgumentNullException.ThrowIfNull(tableName);

        if (_pathsByTableName.TryGetValue(tableName, out var path))
            return File.OpenRead(path);

        throw new FileNotFoundException($"No .db2 file found for table '{tableName}' in '{options.Db2DirectoryPath}'.");
    }
}
