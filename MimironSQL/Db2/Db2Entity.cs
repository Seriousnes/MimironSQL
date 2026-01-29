using System.Numerics;

namespace MimironSQL.Db2;

public abstract class Db2Entity<TId> where TId : IEquatable<TId>, IComparable<TId>
{
    public required TId Id { get; set; }
}

public abstract class Db2Entity : Db2Entity<int>
{
}

public abstract class Db2LongEntity : Db2Entity<long>
{
}

public abstract class Db2GuidEntity : Db2Entity<Guid>
{
}

public abstract class Db2StringEntity : Db2Entity<string>
{
}