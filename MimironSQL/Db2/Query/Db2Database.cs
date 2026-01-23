using MimironSQL.Db2.Schema;
using MimironSQL.Db2.Wdc5;
using MimironSQL.Providers;
using System;
using System.IO;

namespace MimironSQL.Db2.Query;

public sealed class Db2Database(IDbdProvider dbdProvider)
{
    private readonly SchemaMapper _schemaMapper = new(dbdProvider);

    public Db2Table<T> OpenTable<T>(string tableName, Stream db2Stream, Wdc5FileOptions? options = null)
    {
        ArgumentNullException.ThrowIfNull(tableName);
        ArgumentNullException.ThrowIfNull(db2Stream);

        var file = options is null ? new Wdc5File(db2Stream) : new Wdc5File(db2Stream, options);
        var schema = _schemaMapper.GetSchema(tableName, file);
        return new Db2Table<T>(file, schema);
    }
}
