using MimironSQL.Db2;

namespace MimironSQL.Tests.Fixtures;

internal class Spell : Wdc5Entity
{
    public SpellName? SpellName { get; set; }

    public string NameSubtext_lang { get; set; } = string.Empty;
    public string Description_lang { get; set; } = string.Empty;
    public string AuraDescription_lang { get; set; } = string.Empty;
}
