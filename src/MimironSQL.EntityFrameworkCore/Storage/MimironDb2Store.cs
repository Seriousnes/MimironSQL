using System.Collections.Concurrent;
using MimironSQL.EntityFrameworkCore.Infrastructure;
using MimironSQL.Formats;
using MimironSQL.Providers;

using Microsoft.EntityFrameworkCore.Infrastructure;
using MimironSQL.EntityFrameworkCore.Query.Internal;
using MimironSQL.EntityFrameworkCore.Model;
using MimironSQL.EntityFrameworkCore.Schema;

namespace MimironSQL.EntityFrameworkCore.Storage;

internal sealed class MimironDb2Store : IMimironDb2Store, IDisposable
{
    private readonly IDb2StreamProvider _db2StreamProvider;
    private readonly IDbdProvider _dbdProvider;
    private readonly IDb2Format _format;
    private readonly SchemaMapper _schemaMapper;
    private readonly string _wowVersion;
    private readonly bool _relaxLayoutValidation;
    private readonly ConcurrentDictionary<string, Lazy<Db2TableSchema>> _schemaCache = new(StringComparer.OrdinalIgnoreCase);
    private readonly ConcurrentDictionary<string, Lazy<(IDb2File File, Db2TableSchema Schema)>> _fileCache = new(StringComparer.OrdinalIgnoreCase);

    public MimironDb2Store(
        IDb2StreamProvider db2StreamProvider,
        IDbdProvider dbdProvider,
        IDb2Format format,
        IDbContextOptions contextOptions)
    {
        ArgumentNullException.ThrowIfNull(db2StreamProvider);
        ArgumentNullException.ThrowIfNull(dbdProvider);
        ArgumentNullException.ThrowIfNull(format);
        ArgumentNullException.ThrowIfNull(contextOptions);

        _db2StreamProvider = db2StreamProvider;
        _dbdProvider = dbdProvider;
        _format = format;

        var extension = contextOptions.Extensions.OfType<MimironDb2OptionsExtension>().First();

        _wowVersion = extension.WowVersion!;
        _relaxLayoutValidation = extension.RelaxLayoutValidation;
        _schemaMapper = new SchemaMapper(dbdProvider, _wowVersion);
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
        ArgumentException.ThrowIfNullOrWhiteSpace(tableName);

        var lazy = _schemaCache.GetOrAdd(tableName, key => new Lazy<Db2TableSchema>(() =>
        {
            return _schemaMapper.GetSchema(key);
        }));

        return lazy.Value;
    }

    public (IDb2File File, Db2TableSchema Schema) OpenTableWithSchema(string tableName)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(tableName);

        var lazy = _fileCache.GetOrAdd(tableName, key => new Lazy<(IDb2File File, Db2TableSchema Schema)>(() =>
        {
            var stream = _db2StreamProvider.OpenDb2Stream(key);
            try
            {
                var file = _format.OpenFile(stream);
                var layout = _format.GetLayout(file);

                var schema = GetSchema(key);
                ValidateLayout(key, schema, layout);
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

    private void ValidateLayout(string tableName, Db2TableSchema expectedSchema, Db2FileLayout actualLayout)
    {
        if (_relaxLayoutValidation)
            return;

        if (!expectedSchema.IsLayoutHashAllowed(actualLayout.LayoutHash))
        {
            var allowed = expectedSchema.AllowedLayoutHashes is null
                ? "<any>"
                : string.Join(", ", expectedSchema.AllowedLayoutHashes.Select(static h => h.ToString("X8")));

            throw new InvalidDataException(
                $"DB2 layout hash mismatch for '{tableName}'. " +
                $"WOW_VERSION={_wowVersion}. Actual={actualLayout.LayoutHash:X8}. Allowed={allowed}.");
        }

        if (expectedSchema.PhysicalColumnCount != actualLayout.PhysicalFieldsCount)
        {
            throw new InvalidDataException(
                $"DB2 physical column count mismatch for '{tableName}'. " +
                $"WOW_VERSION={_wowVersion}. Actual={actualLayout.PhysicalFieldsCount}. Expected={expectedSchema.PhysicalColumnCount}."
            );
        }
    }

    public bool TryMaterializeById<TEntity>(string tableName, int id, Db2ModelBinding model, IDb2EntityFactory entityFactory, out TEntity? entity)
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
        var materializer = new Db2EntityMaterializer<TEntity>(model, db2EntityType, entityFactory);

        entity = materializer.Materialize(typed, handle);
        return true;
    }

    public IReadOnlyList<TEntity> MaterializeByIds<TEntity>(string tableName, IReadOnlyList<int> ids, int? takeCount, Db2ModelBinding model, IDb2EntityFactory entityFactory)
        where TEntity : class
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(tableName);
        ArgumentNullException.ThrowIfNull(ids);
        ArgumentNullException.ThrowIfNull(model);
        ArgumentNullException.ThrowIfNull(entityFactory);

        if (takeCount is 0 || ids.Count == 0)
            return [];

        if (takeCount is < 0)
            throw new ArgumentOutOfRangeException(nameof(takeCount), "Take count cannot be negative.");

        var maxCount = takeCount ?? int.MaxValue;
        var results = new List<TEntity>(capacity: Math.Min(ids.Count, maxCount));

        var (file, schema) = OpenTableWithSchema(tableName);

        if (file is not IDb2File<RowHandle> typed)
            throw new NotSupportedException($"Key lookups require row type '{typeof(RowHandle).FullName}', but file reports '{file.RowType.FullName}'.");

        var db2EntityType = model.GetEntityType(typeof(TEntity)).WithSchema(tableName, schema);
        var materializer = new Db2EntityMaterializer<TEntity>(model, db2EntityType, entityFactory);

        var handles = new List<RowHandle>(capacity: Math.Min(ids.Count, maxCount));

        for (var i = 0; i < ids.Count; i++)
        {
            var id = ids[i];
            if (!typed.TryGetRowById(id, out var handle))
                continue;

            handles.Add(handle);
            if (handles.Count >= maxCount)
                break;
        }

        if (handles.Count == 0)
            return [];

        handles.Sort(static (a, b) =>
        {
            var section = a.SectionIndex.CompareTo(b.SectionIndex);
            if (section != 0)
                return section;

            return a.RowIndexInSection.CompareTo(b.RowIndexInSection);
        });

        for (var i = 0; i < handles.Count; i++)
        {
            var entity = materializer.Materialize(typed, handles[i]);
            results.Add(entity);
        }

        return results;
    }

    public void Dispose()
    {
        foreach (var entry in _fileCache.Values)
        {
            if (entry.IsValueCreated)
                (entry.Value.File as IDisposable)?.Dispose();
        }

        _fileCache.Clear();
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
