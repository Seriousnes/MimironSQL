using MimironSQL.Db2.Schema;
using MimironSQL.Db2.Wdc5;

namespace MimironSQL.Db2.Query;

internal interface IDb2Table
{
    Type EntityType { get; }
    Wdc5File File { get; }
    Db2TableSchema Schema { get; }
}
