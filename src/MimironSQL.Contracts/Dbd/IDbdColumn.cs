using MimironSQL.Db2;

namespace MimironSQL.Dbd;

/// <summary>
/// Represents a DBD column definition.
/// </summary>
public interface IDbdColumn
{
    /// <summary>
    /// Gets the value type for this column.
    /// </summary>
    Db2ValueType ValueType { get; }

    /// <summary>
    /// Gets the referenced table name, if this column is a reference.
    /// </summary>
    string? ReferencedTableName { get; }

    /// <summary>
    /// Gets whether this column has been verified against the DB2 layout metadata.
    /// </summary>
    bool IsVerified { get; }
}
