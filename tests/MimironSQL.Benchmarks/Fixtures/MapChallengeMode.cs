namespace MimironSQL.Benchmarks.Fixtures;

public class MapChallengeMode
{
    public int Id { get; set; }

    public int MapID { get; set; }
    public Map? Map { get; set; }

    public string Name_lang { get; set; } = string.Empty;
}
