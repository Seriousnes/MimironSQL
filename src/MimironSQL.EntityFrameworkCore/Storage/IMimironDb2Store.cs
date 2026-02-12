using MimironSQL.EntityFrameworkCore.Db2.Schema;
using MimironSQL.EntityFrameworkCore.Db2.Model;
using MimironSQL.EntityFrameworkCore.Db2.Query;
using MimironSQL.Formats;

namespace MimironSQL.EntityFrameworkCore.Storage;

internal interface IMimironDb2Store
{
    IDb2File OpenTable(string tableName);

    IDb2File<TRow> OpenTable<TRow>(string tableName) where TRow : struct;

    Db2TableSchema GetSchema(string tableName);

    (IDb2File File, Db2TableSchema Schema) OpenTableWithSchema(string tableName);

    (IDb2File<TRow> File, Db2TableSchema Schema) OpenTableWithSchema<TRow>(string tableName) where TRow : struct;

    bool TryMaterializeById<TEntity>(string tableName, int id, Db2ModelBinding model, IDb2EntityFactory entityFactory, out TEntity? entity)
        where TEntity : class;

    IReadOnlyList<TEntity> MaterializeByIds<TEntity>(string tableName, IReadOnlyList<int> ids, int? takeCount, Db2ModelBinding model, IDb2EntityFactory entityFactory)
        where TEntity : class;
}
