using MimironSQL.Db2;

namespace MimironSQL.Tests.Fixtures;

internal sealed class MapChallengeMode : Wdc5Entity
{
    public ushort MapID { get; set; }

    public Map? Map { get; set; }

    public string Name_lang { get; set; } = string.Empty;
}
