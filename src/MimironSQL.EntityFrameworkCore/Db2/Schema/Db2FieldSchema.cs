using System.Diagnostics.CodeAnalysis;

namespace MimironSQL.Db2.Schema;

[ExcludeFromCodeCoverage]
public readonly record struct Db2FieldSchema(
    string Name,
    Db2ValueType ValueType,
    int ColumnStartIndex,
    int ElementCount,
    bool IsVerified,
    bool IsVirtual,
    bool IsId,
    bool IsRelation,
    string? ReferencedTableName);
