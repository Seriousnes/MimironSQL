using System.Numerics;

namespace MimironSQL.Db2;

public abstract class Wdc5Entity<TId> where TId : IBinaryInteger<TId>
{
    public TId Id { get; set; } = TId.Zero;
}

public abstract class Wdc5Entity : Wdc5Entity<int>
{
}
