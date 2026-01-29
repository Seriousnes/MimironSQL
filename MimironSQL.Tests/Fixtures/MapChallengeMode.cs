using MimironSQL.Db2;

namespace MimironSQL.Tests.Fixtures;

internal class MapChallengeMode : Db2Entity
{
    public ushort MapID { get; set; }
    public Map? Map { get; set; }
    public string Name_lang { get; set; } = string.Empty;    
    public ICollection<int> FirstRewardQuestID { get; set; }
    public ICollection<QuestV2> FirstRewardQuest { get; set; }
}

internal class QuestV2 : Db2Entity
{
    public long UniqueBitFlag { get; set; } 
    public int UiQUestDetailsThemeID { get; set; }
}
