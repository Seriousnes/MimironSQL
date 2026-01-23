using MimironSQL.Db2.Schema;
using MimironSQL.Db2.Wdc5;
using MimironSQL.Providers;

using System.Reflection;

namespace MimironSQL.Db2.Query;

public abstract class Db2Context
{
    private static readonly Type Db2TableOpenGenericType = typeof(Db2Table<>);

    private readonly SchemaMapper _schemaMapper;
    private readonly IDb2StreamProvider _db2StreamProvider;
    private readonly Dictionary<string, (Wdc5File File, Db2TableSchema Schema)> _cache = new(StringComparer.OrdinalIgnoreCase);

    protected Db2Context(IDbdProvider dbdProvider, IDb2StreamProvider db2StreamProvider)
    {
        _schemaMapper = new SchemaMapper(dbdProvider);
        _db2StreamProvider = db2StreamProvider;

        InitializeTableProperties();
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
            var tableName = entityType.GetCustomAttribute<Db2TableNameAttribute>(inherit: true)?.TableName ?? entityType.Name;

            var table = OpenTable(entityType, tableName);
            p.SetValue(this, table);
        }
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
        if (_cache.TryGetValue(tableName, out var cached))
            return new Db2Table<T>(cached.File, cached.Schema);

        using var stream = _db2StreamProvider.OpenDb2Stream(tableName);
        var file = new Wdc5File(stream);
        var schema = _schemaMapper.GetSchema(tableName, file);
        _cache[tableName] = (file, schema);
        return new Db2Table<T>(file, schema);
    }

    protected Db2Table<T> Table<T>(string? tableName = null)
    {
        return OpenTableGeneric<T>(tableName ?? typeof(T).Name);
    }
}
