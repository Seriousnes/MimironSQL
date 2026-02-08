namespace MimironSQL.Benchmarks.Fixtures;

public class Spell
{
    public int Id { get; set; }

    public SpellName? SpellName { get; set; }

    public string NameSubtext_lang { get; set; } = string.Empty;
    public string Description_lang { get; set; } = string.Empty;
    public string AuraDescription_lang { get; set; } = string.Empty;
}
