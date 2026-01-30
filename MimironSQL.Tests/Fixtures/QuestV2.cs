using MimironSQL.Db2;

namespace MimironSQL.Tests.Fixtures;

internal class QuestV2 : Db2Entity
{
    public long UniqueBitFlag { get; set; } 
    public int UiQuestDetailsThemeID { get; set; }
}
