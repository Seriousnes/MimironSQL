namespace MimironSQL.EntityFrameworkCore;

/// <summary>
/// Controls how FK-array navigations (configured via <c>HasForeignKeyArray</c>) are represented in the EF Core model.
/// </summary>
public enum ForeignKeyArrayModeling
{
    /// <summary>
    /// Use a shared-type join entity backed by a property bag (<see cref="Dictionary{TKey,TValue}" />).
    /// </summary>
    SharedTypeJoinEntity = 0,

    /// <summary>
    /// Use a shared-type join entity backed by a CLR join row type.
    /// </summary>
    ClrJoinEntity = 1,
}
