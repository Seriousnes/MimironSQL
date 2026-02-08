using MimironSQL.Db2.Schema;
using MimironSQL.Formats;

namespace MimironSQL.EntityFrameworkCore.Storage;

internal interface IMimironDb2Store
{
    IDb2File OpenTable(string tableName);
    
    IDb2File<TRow> OpenTable<TRow>(string tableName) where TRow : struct;
    
    Db2TableSchema GetSchema(string tableName);
    
    (IDb2File File, Db2TableSchema Schema) OpenTableWithSchema(string tableName);
    
    (IDb2File<TRow> File, Db2TableSchema Schema) OpenTableWithSchema<TRow>(string tableName) where TRow : struct;
}
