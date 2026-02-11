using System.Collections.Concurrent;

using MimironSQL.Db2;
using MimironSQL.EntityFrameworkCore.Db2.Model;
using MimironSQL.EntityFrameworkCore.Db2.Query;
using MimironSQL.EntityFrameworkCore.Db2.Schema;
using MimironSQL.Formats;
using MimironSQL.Formats.Wdc5.Db2;
using MimironSQL.Providers;

namespace MimironSQL.EntityFrameworkCore.Storage;

internal sealed class MimironDb2Store : IMimironDb2Store
{
    private readonly IDb2StreamProvider _db2StreamProvider;
    private readonly IDbdProvider _dbdProvider;
    private readonly IDb2Format _format;
    private readonly SchemaMapper _schemaMapper;
    private readonly ConcurrentDictionary<string, Lazy<(IDb2File File, Db2TableSchema Schema)>> _cache = new(StringComparer.OrdinalIgnoreCase);
    private readonly ConcurrentDictionary<string, Lazy<Db2TableSchema>> _schemaFromMetadataCache = new(StringComparer.OrdinalIgnoreCase);
    private readonly ConcurrentDictionary<string, Lazy<KeyLookupTable>> _keyLookupCache = new(StringComparer.OrdinalIgnoreCase);

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

    private sealed record KeyLookupTable(Wdc5KeyLookupMetadata Metadata, Db2TableSchema Schema);

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

            try
            {
                var layout = Wdc5LayoutReader.ReadLayout(stream);
                return _schemaMapper.GetSchema(key, layout);
            }
            catch (InvalidDataException)
            {
                // Fallback for non-WDC5 formats: schema resolution requires opening the file.
                // This may populate the full-table cache.
                return GetSchema(key);
            }
        }));

        return lazy.Value;
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

    public bool TryMaterializeById<TEntity>(string tableName, int id, Db2Model model, IDb2EntityFactory entityFactory, out TEntity? entity)
        where TEntity : class
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(tableName);
        ArgumentNullException.ThrowIfNull(model);
        ArgumentNullException.ThrowIfNull(entityFactory);

        var table = _keyLookupCache.GetOrAdd(tableName, key => new Lazy<KeyLookupTable>(() =>
        {
            using var stream = _db2StreamProvider.OpenDb2Stream(key);

            // For now, key lookups are only supported for WDC5 (the only shipped format).
            // This path intentionally avoids allocating full record blobs and dense string tables.
            var metadata = Wdc5KeyLookupMetadata.Parse(stream, options: null);
            var layout = new Db2FileLayout(metadata.Header.LayoutHash, metadata.Header.FieldsCount);
            var schema = _schemaMapper.GetSchema(key, layout);
            return new KeyLookupTable(metadata, schema);
        })).Value;

        if (!table.Metadata.TryResolveRowHandle(id, out var handle, out var resolution))
        {
            entity = null;
            return false;
        }

        var db2EntityType = model.GetEntityType(typeof(TEntity)).WithSchema(tableName, table.Schema);
        var materializer = new Db2EntityMaterializer<TEntity, RowHandle>(db2EntityType, entityFactory);

        using var rowStream = _db2StreamProvider.OpenDb2Stream(tableName);
        var rowFile = new Wdc5KeyLookupRowFile(table.Metadata, resolution, handle, rowStream, options: null);
        entity = materializer.Materialize(rowFile, handle);
        return true;
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
