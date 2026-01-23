namespace MimironSQL.Db2.Schema;

public readonly record struct Db2FieldSchema(
    string Name,
    Db2ValueType ValueType,
    int ColumnStartIndex,
    int ColumnSpan,
    bool IsVirtual,
    bool IsId);
