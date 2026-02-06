using MimironSQL.Db2.Schema;
using MimironSQL.Formats;
using MimironSQL.Providers;

namespace MimironSQL.EntityFrameworkCore.Storage;

public sealed class MimironDb2Store : IMimironDb2Store
{
    private readonly IDb2StreamProvider _db2StreamProvider;
    private readonly IDbdProvider _dbdProvider;
    private readonly IDb2Format _format;
    private readonly SchemaMapper _schemaMapper;
    private readonly Dictionary<string, (IDb2File File, Db2TableSchema Schema)> _cache = new(StringComparer.OrdinalIgnoreCase);
    private readonly Lock _cacheLock = new();

    public MimironDb2Store(IDb2StreamProvider db2StreamProvider, IDbdProvider dbdProvider, IDb2Format format)
    {
        ArgumentNullException.ThrowIfNull(db2StreamProvider);
        ArgumentNullException.ThrowIfNull(dbdProvider);
        ArgumentNullException.ThrowIfNull(format);

        _db2StreamProvider = db2StreamProvider;
        _dbdProvider = dbdProvider;
        _format = format;
        _schemaMapper = new SchemaMapper(dbdProvider);
    }

    public IDb2File OpenTable(string tableName)
    {
        var (file, _) = OpenTableWithSchema(tableName);
        return file;
    }

    public IDb2File<TRow> OpenTable<TRow>(string tableName) where TRow : struct
    {
        var (file, _) = OpenTableWithSchema<TRow>(tableName);
        return file;
    }

    public Db2TableSchema GetSchema(string tableName)
    {
        var (_, schema) = OpenTableWithSchema(tableName);
        return schema;
    }

    public (IDb2File File, Db2TableSchema Schema) OpenTableWithSchema(string tableName)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(tableName);

        lock (_cacheLock)
        {
            if (_cache.TryGetValue(tableName, out var cached))
                return cached;

            using var stream = _db2StreamProvider.OpenDb2Stream(tableName);
            var file = _format.OpenFile(stream);
            var layout = _format.GetLayout(file);
            var schema = _schemaMapper.GetSchema(tableName, layout);
            
            var entry = (file, schema);
            _cache[tableName] = entry;
            return entry;
        }
    }

    public (IDb2File<TRow> File, Db2TableSchema Schema) OpenTableWithSchema<TRow>(string tableName) where TRow : struct
    {
        var (file, schema) = OpenTableWithSchema(tableName);
        return ((IDb2File<TRow>)file, schema);
    }
}
