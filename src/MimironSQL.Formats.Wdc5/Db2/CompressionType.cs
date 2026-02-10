namespace MimironSQL.Formats.Wdc5.Db2;

/// <summary>
/// Defines the column compression mode used by WDC5.
/// </summary>
public enum CompressionType : uint
{
    /// <summary>
    /// Uncompressed storage.
    /// </summary>
    None = 0,

    /// <summary>
    /// Bit-packed immediate encoding.
    /// </summary>
    Immediate = 1,

    /// <summary>
    /// Common-value dictionary encoding.
    /// </summary>
    Common = 2,

    /// <summary>
    /// Pallet (lookup table) encoding.
    /// </summary>
    Pallet = 3,

    /// <summary>
    /// Pallet encoding with multiple values per row.
    /// </summary>
    PalletArray = 4,

    /// <summary>
    /// Signed bit-packed immediate encoding.
    /// </summary>
    SignedImmediate = 5,
}
