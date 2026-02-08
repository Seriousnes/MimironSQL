namespace MimironSQL.Benchmarks.Fixtures;

public class Map
{
    public int Id { get; set; }

    public string Directory { get; set; } = string.Empty;
    public string MapName_lang { get; set; } = string.Empty;

    public ICollection<MapChallengeMode> MapChallengeModes { get; set; } = null!;
}
