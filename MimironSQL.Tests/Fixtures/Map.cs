using MimironSQL.Db2;

namespace MimironSQL.Tests.Fixtures;

internal class Map : Wdc5Entity
{
    public string Directory { get; set; } = string.Empty;
    public string MapName_lang { get; set; } = string.Empty;

    public int ParentMapID { get; set; }
    public Map? ParentMap { get; set; }
}

internal class MapWithCtor : Wdc5Entity
{
    public static int InstancesCreated;

    public MapWithCtor()
    {
        InstancesCreated++;
    }

    public string Directory { get; set; } = string.Empty;
}