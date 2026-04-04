namespace MimironSQL.Formats.Wdc5;

/// <summary>
/// Options controlling higher-level WDC5 behavior exposed through DI.
/// </summary>
public sealed class Wdc5FormatOptions
{
    /// <summary>
    /// Builds sparse field offset tables during file construction instead of on first sparse access.
    /// </summary>
    public bool EagerSparseOffsetTable { get; set; }
}