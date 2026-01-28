using MimironSQL.Db2;

namespace MimironSQL.Tests.Fixtures;

internal class CollectableSourceQuestSparse : Db2Entity
{
    public int QuestID { get; set; }
    public int CollectableSourceInfoID { get; set; }
}
