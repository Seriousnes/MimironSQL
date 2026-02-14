using MimironSQL.EntityFrameworkCore.Db2.Schema;

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
}
