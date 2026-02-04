using MimironSQL.Db2;

namespace MimironSQL.Dbd;

public interface IDbdLayoutEntry
{
    string Name { get; }

    Db2ValueType ValueType { get; }

    string? ReferencedTableName { get; }

    int ElementCount { get; }

    bool IsVerified { get; }

    bool IsNonInline { get; }

    bool IsId { get; }

    bool IsRelation { get; }

    string? InlineTypeToken { get; }
}
