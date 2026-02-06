using MimironSQL.Providers;

namespace MimironSQL.Formats.Wdc5;

public sealed record Wdc5FileOptions(
    ITactKeyProvider? TactKeyProvider = null,
    Wdc5EncryptedRowNonceStrategy EncryptedRowNonceStrategy = Wdc5EncryptedRowNonceStrategy.SourceId);

public enum Wdc5EncryptedRowNonceStrategy
{
    DestinationId,
    SourceId,
}
