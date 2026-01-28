using MimironSQL.Db2.Model;
using MimironSQL.Db2.Schema;
using MimironSQL.Db2.Wdc5;
using MimironSQL.Providers;

using System.Reflection;

namespace MimironSQL.Db2.Query;

public abstract class Db2Context
{
    private static readonly Type Db2TableOpenGenericType = typeof(Db2Table<>);

    private readonly Db2Model _model;
    private readonly SchemaMapper _schemaMapper;
    private readonly IDb2StreamProvider _db2StreamProvider;
    private readonly Db2ContextQueryProvider _queryProvider;
    private readonly Dictionary<string, (Wdc5File File, Db2TableSchema Schema)> _cache = new(StringComparer.OrdinalIgnoreCase);

    protected Db2Context(IDbdProvider dbdProvider, IDb2StreamProvider db2StreamProvider)
    {
        _schemaMapper = new SchemaMapper(dbdProvider);
        _db2StreamProvider = db2StreamProvider;
        _queryProvider = new Db2ContextQueryProvider(this);

        _model = BuildModel();

        InitializeTableProperties();
    }

    protected virtual void OnModelCreating(Db2ModelBuilder modelBuilder)
    {
    }

    internal Db2Model Model => _model;

    internal (Wdc5File File, Db2TableSchema Schema) GetOrOpenTableRaw(string tableName)
    {
        if (_cache.TryGetValue(tableName, out var cached))
            return cached;

        using var stream = _db2StreamProvider.OpenDb2Stream(tableName);
        var file = new Wdc5File(stream);
        var schema = _schemaMapper.GetSchema(tableName, file);
        _cache[tableName] = (file, schema);
        return (file, schema);
    }

    private void InitializeTableProperties()
    {
        var derivedType = GetType();
        foreach (var p in derivedType.GetProperties(BindingFlags.Instance | BindingFlags.Public))
        {
            if (p.PropertyType is not { IsGenericType: true } pt)
                continue;

            if (pt.GetGenericTypeDefinition() != Db2TableOpenGenericType)
                continue;

            if (p.SetMethod is not { IsPublic: true })
                continue;

            var entityType = pt.GetGenericArguments()[0];
            var tableName = _model.TryGetEntityType(entityType, out var configured)
                ? configured.TableName
                : entityType.Name;

            var table = OpenTable(entityType, tableName);
            p.SetValue(this, table);
        }
    }

    private Db2Model BuildModel()
    {
        var builder = new Db2ModelBuilder();
        builder.ApplyTablePropertyConventions(GetType());
        OnModelCreating(builder);

        builder.ApplySchemaNavigationConventions(tableName => GetOrOpenTableRaw(tableName).Schema);
        return builder.Build(tableName => GetOrOpenTableRaw(tableName).Schema);
    }

    private object OpenTable(Type entityType, string tableName)
    {
        ArgumentNullException.ThrowIfNull(entityType);
        ArgumentNullException.ThrowIfNull(tableName);

        var m = typeof(Db2Context).GetMethod(nameof(OpenTableGeneric), BindingFlags.Instance | BindingFlags.NonPublic)!;
        return m.MakeGenericMethod(entityType).Invoke(this, [tableName])!;
    }

    private Db2Table<T> OpenTableGeneric<T>(string tableName)
    {
        var (file, schema) = GetOrOpenTableRaw(tableName);
        return new Db2Table<T>(tableName, schema, _queryProvider, name => GetOrOpenTableRaw(name).File);
    }

    protected Db2Table<T> Table<T>(string? tableName = null)
    {
        if (tableName is not null)
            return OpenTableGeneric<T>(tableName);

        return _model.TryGetEntityType(typeof(T), out var configured)
            ? OpenTableGeneric<T>(configured.TableName)
            : OpenTableGeneric<T>(typeof(T).Name);
    }
}

