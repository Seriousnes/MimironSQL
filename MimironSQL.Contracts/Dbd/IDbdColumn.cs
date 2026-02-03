using MimironSQL.Db2;

namespace MimironSQL.Dbd;

public interface IDbdColumn
{
    Db2ValueType ValueType { get; }

    string? ReferencedTableName { get; }

    bool IsVerified { get; }
}
