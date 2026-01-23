using MimironSQL.Db2;

namespace MimironSQL.Tests.Fixtures;

internal class AccountStoreCategory : Wdc5Entity
{
    public int StoreFrontID { get; set; }
    public string Name_lang { get; set; } = string.Empty;
}
