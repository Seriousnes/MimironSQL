using MimironSQL.Db2;

namespace MimironSQL.Benchmarks.Fixtures;

public class SpellName : Db2Entity
{
    public string Name_lang { get; set; } = string.Empty;
}
