namespace MimironSQL.EntityFrameworkCore.Model;

/// <summary>
/// Base type for DB2 entity CLR types.
/// </summary>
/// <typeparam name="TKey">The primary key CLR type.</typeparam>
public abstract class Db2Entity<TKey> where TKey : IEquatable<TKey>, IComparable<TKey>
{
    /// <summary>
    /// Gets or sets the primary key value.
    /// </summary>
    public virtual TKey Id { get; set; } = default!;
}
