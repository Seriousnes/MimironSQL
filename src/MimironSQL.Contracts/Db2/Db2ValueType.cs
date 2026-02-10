namespace MimironSQL.Db2;

/// <summary>
/// Value types that can appear in DBD definitions and DB2 layouts.
/// </summary>
public enum Db2ValueType
{
    /// <summary>Unknown value type.</summary>
    Unknown = 0,

    /// <summary>Signed 64-bit integer.</summary>
    Int64,

    /// <summary>Unsigned 64-bit integer.</summary>
    UInt64,

    /// <summary>32-bit floating point number.</summary>
    Single,

    /// <summary>String value.</summary>
    String,

    /// <summary>Localized string value.</summary>
    LocString,
}
