using MimironSQL.Db2;

namespace MimironSQL;

public class TactKeyEntity : Db2Entity<int>
{
    public byte[] Key { get; set; } = [];
}
