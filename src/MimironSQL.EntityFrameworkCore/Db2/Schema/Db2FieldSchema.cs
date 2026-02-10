using System.Diagnostics.CodeAnalysis;

using MimironSQL.Db2;

namespace MimironSQL.EntityFrameworkCore.Db2.Schema;

/// <summary>
/// Describes a DB2 field as mapped by the EF Core provider.
/// </summary>
/// <param name="Name">The field name.</param>
/// <param name="ValueType">The DB2 value type.</param>
/// <param name="ColumnStartIndex">The starting physical column index for this field.</param>
/// <param name="ElementCount">The number of elements for array-like fields.</param>
/// <param name="IsVerified">Whether the schema entry was verified against DBD metadata.</param>
/// <param name="IsVirtual">Whether this field is virtual (not directly backed by a physical column).</param>
/// <param name="IsId">Whether this field represents the row ID.</param>
/// <param name="IsRelation">Whether this field represents a relation/foreign key.</param>
/// <param name="ReferencedTableName">The referenced table name for relation fields, if any.</param>
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
