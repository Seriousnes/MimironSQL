namespace MimironSQL.EntityFrameworkCore.Index;

internal sealed class Db2IndexCacheLocator
{
    private readonly string? _overrideDirectory;

    public Db2IndexCacheLocator(string? overrideDirectory)
    {
        _overrideDirectory = overrideDirectory;
    }

    public string GetIndexDirectory(string wowVersion)
    {
        var baseDir = _overrideDirectory
            ?? Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData), "MimironSQL", "indexes");

        var dir = Path.Combine(baseDir, SanitizePathSegment(wowVersion));
        Directory.CreateDirectory(dir);
        return dir;
    }

    public string GetIndexFilePath(string wowVersion, string tableName, string fieldName, uint layoutHash)
    {
        var dir = GetIndexDirectory(wowVersion);
        var fileName = $"{SanitizePathSegment(tableName)}_{SanitizePathSegment(fieldName)}_{layoutHash:X8}.db2idx";
        return Path.Combine(dir, fileName);
    }

    private static string SanitizePathSegment(string value)
    {
        foreach (var c in Path.GetInvalidFileNameChars())
        {
            value = value.Replace(c, '_');
        }

        return value;
    }
}
