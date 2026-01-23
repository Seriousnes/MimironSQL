using MimironSQL.Db2;
using MimironSQL.Db2.Query;

namespace MimironSQL.Tests.Fixtures;

internal class Map : Wdc5Entity
{
    public string Directory { get; set; } = string.Empty;
    public string MapName_lang { get; set; } = string.Empty;
}

[Db2TableName("Map")]
internal class MapWithCtor : Wdc5Entity
{
    public static int InstancesCreated;

    public MapWithCtor()
    {
        InstancesCreated++;
    }

    public string Directory { get; set; } = string.Empty;
}