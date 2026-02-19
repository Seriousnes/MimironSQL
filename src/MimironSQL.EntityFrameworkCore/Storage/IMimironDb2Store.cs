using MimironSQL.EntityFrameworkCore.Model;
using MimironSQL.EntityFrameworkCore.Query.Internal;
using MimironSQL.EntityFrameworkCore.Schema;
using MimironSQL.Formats;

namespace MimironSQL.EntityFrameworkCore.Storage;

/// <summary>
/// Minimal store abstraction for resolving DB2 schemas and, later, opening DB2 tables.
/// </summary>
internal interface IMimironDb2Store
{
    /// <summary>
    /// Resolves the schema for a DB2 table.
    /// </summary>
    Db2TableSchema GetSchema(string tableName);

    /// <summary>
    /// Opens a DB2 table and returns the file + schema. Implementations may cache per DbContext.
    /// </summary>
    (IDb2File File, Db2TableSchema Schema) OpenTableWithSchema(string tableName);

    /// <summary>
    /// Opens a DB2 table (typed row handles) and returns the file + schema. Implementations may cache per DbContext.
    /// </summary>
    (IDb2File<TRow> File, Db2TableSchema Schema) OpenTableWithSchema<TRow>(string tableName) where TRow : struct;

    bool TryMaterializeById<TEntity>(string tableName, int id, Db2ModelBinding model, IDb2EntityFactory entityFactory, out TEntity? entity)
        where TEntity : class;

    IReadOnlyList<TEntity> MaterializeByIds<TEntity>(string tableName, IReadOnlyList<int> ids, int? takeCount, Db2ModelBinding model, IDb2EntityFactory entityFactory)
        where TEntity : class;
}
