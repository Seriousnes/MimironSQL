using MimironSQL.Db2.Query;
using MimironSQL.Providers;

namespace MimironSQL.Tests.Fixtures;

internal class TestDb2Context(IDbdProvider dbdProvider, IDb2StreamProvider db2StreamProvider) : Db2Context(dbdProvider, db2StreamProvider)
{
    public Db2Table<Map> Map { get; init; } = null!;
    public Db2Table<Spell> Spell { get; init; } = null!;
    public Db2Table<GarrType> GarrType { get; init; } = null!;
    public Db2Table<CollectableSourceQuestSparse> CollectableSourceQuestSparse { get; init; } = null!;
    public Db2Table<AccountStoreCategory> AccountStoreCategory { get; init; } = null!;
}
