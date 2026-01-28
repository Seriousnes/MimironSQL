using System.Numerics;

namespace MimironSQL.Db2;

public abstract class Db2Entity<TId> where TId : IBinaryInteger<TId>
{
    public TId Id { get; set; } = TId.Zero;
}

public abstract class Db2Entity : Db2Entity<int>
{
}
