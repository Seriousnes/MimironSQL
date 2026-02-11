using System.Collections.Concurrent;

using MimironSQL.Db2;
using MimironSQL.EntityFrameworkCore.Db2.Model;
using MimironSQL.EntityFrameworkCore.Db2.Query;
using MimironSQL.EntityFrameworkCore.Db2.Schema;
using MimironSQL.Formats;
using MimironSQL.Providers;

namespace MimironSQL.EntityFrameworkCore.Storage;

internal sealed class MimironDb2Store : IMimironDb2Store, IDisposable
{
    private readonly IDb2StreamProvider _db2StreamProvider;
    private readonly IDbdProvider _dbdProvider;
    private readonly IDb2Format _format;
    private readonly SchemaMapper _schemaMapper;
    private readonly ConcurrentDictionary<string, Lazy<(IDb2File File, Db2TableSchema Schema)>> _cache = new(StringComparer.OrdinalIgnoreCase);
    private readonly ConcurrentDictionary<string, Lazy<Db2TableSchema>> _schemaFromMetadataCache = new(StringComparer.OrdinalIgnoreCase);

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

    public Db2TableSchema GetSchemaFromMetadata(string tableName)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(tableName);

        var lazy = _schemaFromMetadataCache.GetOrAdd(tableName, key => new Lazy<Db2TableSchema>(() =>
        {
            using var stream = _db2StreamProvider.OpenDb2Stream(key);

            var layout = _format.GetLayout(stream);
            return _schemaMapper.GetSchema(key, layout);
        }));

        return lazy.Value;
    }

    public (IDb2File File, Db2TableSchema Schema) OpenTableWithSchema(string tableName)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(tableName);

        var lazy = _cache.GetOrAdd(tableName, key => new Lazy<(IDb2File File, Db2TableSchema Schema)>(() =>
        {
            var stream = _db2StreamProvider.OpenDb2Stream(key);
            try
            {
                var file = _format.OpenFile(stream);
                var layout = _format.GetLayout(file);
                var schema = _schemaMapper.GetSchema(key, layout);
                return (file, schema);
            }
            catch
            {
                stream.Dispose();
                throw;
            }
        }));

        return lazy.Value;
    }

    public bool TryMaterializeById<TEntity>(string tableName, int id, Db2Model model, IDb2EntityFactory entityFactory, out TEntity? entity)
        where TEntity : class
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(tableName);
        ArgumentNullException.ThrowIfNull(model);
        ArgumentNullException.ThrowIfNull(entityFactory);

        var (file, schema) = OpenTableWithSchema(tableName);

        if (file is not IDb2File<RowHandle> typed)
            throw new NotSupportedException($"Key lookups require row type '{typeof(RowHandle).FullName}', but file reports '{file.RowType.FullName}'.");

        if (!typed.TryGetRowById(id, out var handle))
        {
            entity = null;
            return false;
        }

        var db2EntityType = model.GetEntityType(typeof(TEntity)).WithSchema(tableName, schema);
        var materializer = new Db2EntityMaterializer<TEntity, RowHandle>(db2EntityType, entityFactory);

        entity = materializer.Materialize(typed, handle);
        return true;
    }

    public void Dispose()
    {
        foreach (var lazy in _cache.Values)
        {
            if (!lazy.IsValueCreated)
                continue;

            if (lazy.Value.File is IDisposable disposable)
                disposable.Dispose();
        }
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
