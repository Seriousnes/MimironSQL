using MimironSQL.Db2;

namespace MimironSQL.Dbd;

/// <summary>
/// Represents a single layout entry in a DBD build block.
/// </summary>
public interface IDbdLayoutEntry
{
    /// <summary>
    /// Gets the entry name.
    /// </summary>
    string Name { get; }

    /// <summary>
    /// Gets the entry value type.
    /// </summary>
    Db2ValueType ValueType { get; }

    /// <summary>
    /// Gets the referenced table name, if applicable.
    /// </summary>
    string? ReferencedTableName { get; }

    /// <summary>
    /// Gets the element count for array-like entries.
    /// </summary>
    int ElementCount { get; }

    /// <summary>
    /// Gets whether this entry has been verified against the DB2 layout metadata.
    /// </summary>
    bool IsVerified { get; }

    /// <summary>
    /// Gets whether this entry is stored out-of-line.
    /// </summary>
    bool IsNonInline { get; }

    /// <summary>
    /// Gets whether this entry is an ID column.
    /// </summary>
    bool IsId { get; }

    /// <summary>
    /// Gets whether this entry represents a relation.
    /// </summary>
    bool IsRelation { get; }

    /// <summary>
    /// Gets the inline type token, if present.
    /// </summary>
    string? InlineTypeToken { get; }
}
