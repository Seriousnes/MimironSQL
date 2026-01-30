using MimironSQL.Db2;

namespace MimironSQL.Benchmarks.Fixtures;

public class Map : Db2Entity
{
    public string Directory { get; set; } = string.Empty;
    public string MapName_lang { get; set; } = string.Empty;

    public ICollection<MapChallengeMode> MapChallengeModes { get; set; } = null!;
}
