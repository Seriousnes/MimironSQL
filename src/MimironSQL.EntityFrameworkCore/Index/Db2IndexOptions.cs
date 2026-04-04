namespace MimironSQL.EntityFrameworkCore;

/// <summary>
/// Options for the MimironSQL custom column index feature. Pass to
/// <see cref="MimironDb2IndexExtensions.WithCustomIndexes"/> to override defaults.
/// </summary>
public sealed class Db2IndexOptions
{
    /// <summary>
    /// Overrides the index cache directory. When <see langword="null"/> (the default), indexes are
    /// stored in <c>%LOCALAPPDATA%\MimironSQL\indexes\{wowVersion}\</c>.
    /// </summary>
    public string? CacheDirectory { get; set; }
}
