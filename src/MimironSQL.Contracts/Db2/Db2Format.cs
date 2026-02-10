namespace MimironSQL.Db2;

/// <summary>
/// Known DB2 binary format variants.
/// </summary>
public enum Db2Format
{
    /// <summary>Unknown or unsupported format.</summary>
    Unknown = 0,

    /// <summary>WDC3 format.</summary>
    Wdc3,

    /// <summary>WDC4 format.</summary>
    Wdc4,

    /// <summary>WDC5 format.</summary>
    Wdc5,
}
