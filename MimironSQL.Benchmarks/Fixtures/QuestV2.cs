using MimironSQL.Db2;

namespace MimironSQL.Benchmarks.Fixtures;

public class QuestV2 : Db2Entity
{
    public long UniqueBitFlag { get; set; }
    public int UiQuestDetailsThemeID { get; set; }
}
