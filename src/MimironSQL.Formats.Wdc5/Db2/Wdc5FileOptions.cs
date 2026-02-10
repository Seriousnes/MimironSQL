using MimironSQL.Providers;

namespace MimironSQL.Formats.Wdc5.Db2;

/// <summary>
/// Options controlling WDC5 parsing and decryption behavior.
/// </summary>
/// <param name="TactKeyProvider">Provides TACT keys used to decrypt encrypted sections.</param>
/// <param name="EncryptedRowNonceStrategy">Controls how per-row nonces are derived for decryption.</param>
public sealed record Wdc5FileOptions(
    ITactKeyProvider? TactKeyProvider = null,
    Wdc5EncryptedRowNonceStrategy EncryptedRowNonceStrategy = Wdc5EncryptedRowNonceStrategy.SourceId);

/// <summary>
/// Selects which row identifier is used to derive the decryption nonce.
/// </summary>
public enum Wdc5EncryptedRowNonceStrategy
{
    /// <summary>
    /// Use the destination (post-copy) row ID.
    /// </summary>
    DestinationId,

    /// <summary>
    /// Use the source row ID from the raw record.
    /// </summary>
    SourceId,
}
