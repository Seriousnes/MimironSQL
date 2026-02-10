namespace MimironSQL.Db2;

/// <summary>
/// Flags describing DB2 file characteristics.
/// </summary>
[Flags]
public enum Db2Flags : ushort
{
    /// <summary>No flags.</summary>
    None = 0x0,

    /// <summary>File uses a sparse data layout.</summary>
    Sparse = 0x1,

    /// <summary>File contains a secondary key.</summary>
    SecondaryKey = 0x2,

    /// <summary>File contains an index.</summary>
    Index = 0x4,

    /// <summary>Unspecified flag value.</summary>
    Unknown1 = 0x8,

    /// <summary>File uses bit-packed storage for record data.</summary>
    BitPacked = 0x10,
}
