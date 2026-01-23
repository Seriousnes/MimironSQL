using MimironSQL.Db2.Schema;
using MimironSQL.Db2.Wdc5;
using MimironSQL.Providers;
using System;
using System.Collections.Generic;
using System.IO;

namespace MimironSQL.Db2.Query;

public sealed class Db2Database(IDbdProvider dbdProvider, IDb2StreamProvider? db2StreamProvider = null)
{
    private readonly SchemaMapper _schemaMapper = new(dbdProvider);
    private readonly IDb2StreamProvider? _db2StreamProvider = db2StreamProvider;

    private readonly Dictionary<string, (Wdc5File File, Db2TableSchema Schema)> _cache = new(StringComparer.OrdinalIgnoreCase);

    public Db2Table<T> OpenTable<T>(string tableName, Wdc5FileOptions? options = null)
    {
        ArgumentNullException.ThrowIfNull(tableName);
        if (_db2StreamProvider is null)
            throw new NotSupportedException("Db2Database was constructed without an IDb2StreamProvider.");

        if (_cache.TryGetValue(tableName, out var cached))
            return new Db2Table<T>(cached.File, cached.Schema);

        using var stream = _db2StreamProvider.OpenDb2Stream(tableName);
        var file = options is null ? new Wdc5File(stream) : new Wdc5File(stream, options);
        var schema = _schemaMapper.GetSchema(tableName, file);
        _cache[tableName] = (file, schema);
        return new Db2Table<T>(file, schema);
    }

    public Db2Table<T> OpenTable<T>(string tableName, Stream db2Stream, Wdc5FileOptions? options = null)
    {
        ArgumentNullException.ThrowIfNull(tableName);
        ArgumentNullException.ThrowIfNull(db2Stream);

        if (_cache.TryGetValue(tableName, out var cached))
            return new Db2Table<T>(cached.File, cached.Schema);

        var file = options is null ? new Wdc5File(db2Stream) : new Wdc5File(db2Stream, options);
        var schema = _schemaMapper.GetSchema(tableName, file);
        _cache[tableName] = (file, schema);
        return new Db2Table<T>(file, schema);
    }
}
