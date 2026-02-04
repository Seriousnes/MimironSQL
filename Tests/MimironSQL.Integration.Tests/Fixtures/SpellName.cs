using MimironSQL.Db2;

namespace MimironSQL.Tests.Fixtures;

internal sealed class SpellName : Db2Entity
{
    public string Name_lang { get; set; } = string.Empty;
}
