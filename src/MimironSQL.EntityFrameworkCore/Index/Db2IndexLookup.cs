using MimironSQL.EntityFrameworkCore.Schema;
using MimironSQL.Formats;
using MimironSQL.Formats.Wdc5.Index;

namespace MimironSQL.EntityFrameworkCore.Index;

internal sealed class Db2IndexLookup
{
    private readonly Db2IndexCacheLocator _cacheLocator;
    private readonly string _wowVersion;

    public Db2IndexLookup(Db2IndexCacheLocator cacheLocator, string wowVersion)
    {
        ArgumentNullException.ThrowIfNull(cacheLocator);
        ArgumentException.ThrowIfNullOrWhiteSpace(wowVersion);
        _cacheLocator = cacheLocator;
        _wowVersion = wowVersion;
    }

    public List<RowHandle>? TryFindEquals(
        string tableName,
        Db2FieldSchema fieldSchema,
        uint layoutHash,
        ulong encodedTarget)
    {
        var indexFilePath = _cacheLocator.GetIndexFilePath(_wowVersion, tableName, fieldSchema.Name, layoutHash);
        if (!File.Exists(indexFilePath))
        {
            return null;
        }

        try
        {
            using var reader = new Db2IndexReader(indexFilePath);
            return reader.FindEquals(encodedTarget);
        }
        catch
        {
            return null;
        }
    }

    public List<RowHandle>? TryFindRange(
        string tableName,
        Db2FieldSchema fieldSchema,
        uint layoutHash,
        ulong loEncoded,
        ulong hiEncoded)
    {
        var indexFilePath = _cacheLocator.GetIndexFilePath(_wowVersion, tableName, fieldSchema.Name, layoutHash);
        if (!File.Exists(indexFilePath))
        {
            return null;
        }

        try
        {
            using var reader = new Db2IndexReader(indexFilePath);
            return reader.FindRange(loEncoded, hiEncoded);
        }
        catch
        {
            return null;
        }
    }
}
