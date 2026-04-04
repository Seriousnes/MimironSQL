using MimironSQL.Providers;

namespace MimironSQL.Formats.Wdc5.Db2;

/// <summary>
/// Controls how record data (and dense string tables) are loaded.
/// </summary>
public enum Wdc5RecordLoadingMode
{
    /// <summary>
    /// Parse metadata up-front and load record/string bytes on demand.
    /// </summary>
    Lazy,

    /// <summary>
    /// Fully materialize record and string bytes during construction.
    /// </summary>
    Eager,
}

/// <summary>
/// Options controlling WDC5 parsing and decryption behavior.
/// </summary>
/// <param name="TactKeyProvider">Provides TACT keys used to decrypt encrypted sections.</param>
/// <param name="EncryptedRowNonceStrategy">Controls how per-row nonces are derived for decryption.</param>
/// <param name="RecordLoadingMode">Controls whether record and string data is loaded lazily or eagerly.</param>
/// <param name="EagerSparseOffsetTable">Controls whether sparse field offset tables are built during file construction.</param>
public sealed record Wdc5FileOptions(
    ITactKeyProvider? TactKeyProvider = null,
    Wdc5EncryptedRowNonceStrategy EncryptedRowNonceStrategy = Wdc5EncryptedRowNonceStrategy.SourceId,
    Wdc5RecordLoadingMode RecordLoadingMode = Wdc5RecordLoadingMode.Lazy,
    bool EagerSparseOffsetTable = false);

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
