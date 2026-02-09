using System.Collections.Concurrent;

using MimironSQL.EntityFrameworkCore.Db2.Schema;
using MimironSQL.Formats;
using MimironSQL.Providers;

namespace MimironSQL.EntityFrameworkCore.Storage;

internal sealed class MimironDb2Store : IMimironDb2Store
{
    private readonly IDb2StreamProvider _db2StreamProvider;
    private readonly IDbdProvider _dbdProvider;
    private readonly IDb2Format _format;
    private readonly SchemaMapper _schemaMapper;
    private readonly ConcurrentDictionary<string, Lazy<(IDb2File File, Db2TableSchema Schema)>> _cache = new(StringComparer.OrdinalIgnoreCase);

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

        var lazy = _cache.GetOrAdd(tableName, key => new Lazy<(IDb2File File, Db2TableSchema Schema)>(() =>
        {
            using var stream = _db2StreamProvider.OpenDb2Stream(key);
            var file = _format.OpenFile(stream);
            var layout = _format.GetLayout(file);
            var schema = _schemaMapper.GetSchema(key, layout);
            return (file, schema);
        }));

        return lazy.Value;
    }

    public (IDb2File<TRow> File, Db2TableSchema Schema) OpenTableWithSchema<TRow>(string tableName) where TRow : struct
    {
        var (file, schema) = OpenTableWithSchema(tableName);

        if (file is IDb2File<TRow> typedFile)
            return (typedFile, schema);

        var requestedType = typeof(TRow);
        var actualType = file.RowType;

        throw new InvalidOperationException(
            $"Requested row type '{requestedType.FullName}' for table '{tableName}', " +
            $"but underlying DB2 file uses row type '{actualType?.FullName ?? "unknown"}'.");
    }
}
