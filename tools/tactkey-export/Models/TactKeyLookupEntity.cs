using MimironSQL.Db2;

namespace MimironSQL;

public class TactKeyLookupEntity : Db2Entity<int>
{
    public byte[] TACTID { get; set; } = [];
}
