using MimironSQL.Providers;

namespace MimironSQL.Db2.Wdc5;

public sealed record Wdc5FileOptions(
    ITactKeyProvider? TactKeyProvider = null,
    Wdc5EncryptedRowNonceStrategy EncryptedRowNonceStrategy = Wdc5EncryptedRowNonceStrategy.DestinationId);

public enum Wdc5EncryptedRowNonceStrategy
{
    DestinationId,
    SourceId,
}
