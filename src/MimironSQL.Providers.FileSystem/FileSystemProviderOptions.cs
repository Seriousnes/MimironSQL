using System.Data.Common;

namespace MimironSQL.Providers;

/// <summary>
/// Combined options for the file-system DB2 and DBD providers, supporting connection string initialization.
/// <para>Supported connection string keys (case-insensitive, with aliases):</para>
/// <list type="bullet">
/// <item><c>Db2DirectoryPath</c>, <c>Db2Directory</c>, <c>Db2 Directory</c></item>
/// <item><c>DbdDefinitionsDirectory</c>, <c>DbdDirectory</c>, <c>Dbd Directory</c></item>
/// </list>
/// </summary>
public sealed record FileSystemProviderOptions
{
    /// <summary>
    /// Creates default file-system provider options.
    /// </summary>
    public FileSystemProviderOptions() { }

    /// <summary>
    /// Creates file-system provider options by parsing a connection string.
    /// <para>Example: <c>Db2Directory=C:\db2;DbdDirectory=C:\dbd</c></para>
    /// </summary>
    /// <param name="connectionString">A semicolon-delimited key=value connection string.</param>
    public FileSystemProviderOptions(string connectionString)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(connectionString);

        var csb = new DbConnectionStringBuilder { ConnectionString = connectionString };

        Db2DirectoryPath = GetString(csb, "Db2DirectoryPath", "Db2Directory", "Db2 Directory") ?? string.Empty;
        DbdDefinitionsDirectory = GetString(csb, "DbdDefinitionsDirectory", "DbdDirectory", "Dbd Directory") ?? string.Empty;
    }

    /// <summary>
    /// Directory containing <c>.db2</c> files.
    /// </summary>
    public string Db2DirectoryPath { get; init; } = string.Empty;

    /// <summary>
    /// Directory containing WoWDBDefs <c>.dbd</c> definition files.
    /// </summary>
    public string DbdDefinitionsDirectory { get; init; } = string.Empty;

    private static string? GetString(DbConnectionStringBuilder csb, params ReadOnlySpan<string> keys)
    {
        foreach (var key in keys)
        {
            if (csb.TryGetValue(key, out var value) && value?.ToString()?.Trim() is { Length: > 0 } trimmed)
                return trimmed;
        }

        return null;
    }
}
