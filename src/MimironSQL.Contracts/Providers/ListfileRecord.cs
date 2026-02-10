namespace MimironSQL.Providers;

/// <summary>
/// Represents a single record from the WoW listfile.
/// </summary>
public sealed class ListfileRecord
{
    /// <summary>
    /// Gets or sets the file data ID.
    /// </summary>
    public int FileDataId { get; set; }

    /// <summary>
    /// Gets or sets the file name (path) for the file data ID.
    /// </summary>
    public string FileName { get; set; } = string.Empty;
}
