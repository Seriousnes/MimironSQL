using System.Data.Common;

namespace MimironSQL.Providers;

/// <summary>
/// Options for configuring the CASC provider.
/// </summary>
public sealed record CascDb2ProviderOptions
{
    /// <summary>
    /// Creates default CASC provider options.
    /// </summary>
    public CascDb2ProviderOptions() { }

    /// <summary>
    /// Creates CASC provider options by parsing a connection string.
    /// Keys are case-insensitive and common aliases are supported.
    /// <para>Example: <c>WowInstallRoot=C:\WoW;Product=wow;DbdDefinitionsDirectory=C:\dbd</c></para>
    /// <para>Supported keys (with aliases):</para>
    /// <list type="bullet">
    /// <item><c>WowInstallRoot</c>, <c>Install Root</c></item>
    /// <item><c>Product</c></item>
    /// <item><c>DbdDefinitionsDirectory</c>, <c>DbdDirectory</c>, <c>Dbd Directory</c></item>
    /// <item><c>ManifestDirectory</c>, <c>Manifest Directory</c></item>
    /// <item><c>ManifestAssetName</c>, <c>Manifest Asset Name</c></item>
    /// <item><c>TactKeyFilePath</c>, <c>Tact Key File</c></item>
    /// <item><c>ThrowOnEncryptedBlockWithoutKey</c>, <c>Strict Tact Keys</c></item>
    /// </list>
    /// </summary>
    /// <param name="connectionString">A semicolon-delimited key=value connection string.</param>
    public CascDb2ProviderOptions(string connectionString)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(connectionString);

        var csb = new DbConnectionStringBuilder { ConnectionString = connectionString };

        WowInstallRoot = GetString(csb, "WowInstallRoot", "Install Root") ?? string.Empty;
        Product = GetString(csb, "Product") ?? "wow";
        DbdDefinitionsDirectory = GetString(csb, "DbdDefinitionsDirectory", "DbdDirectory", "Dbd Directory");
        ManifestDirectory = GetString(csb, "ManifestDirectory", "Manifest Directory") ?? string.Empty;
        ManifestAssetName = GetString(csb, "ManifestAssetName", "Manifest Asset Name") ?? "manifest.json";
        TactKeyFilePath = GetString(csb, "TactKeyFilePath", "Tact Key File");
        ThrowOnEncryptedBlockWithoutKey = GetBool(csb, "ThrowOnEncryptedBlockWithoutKey", "Strict Tact Keys");
    }

    /// <summary>
    /// Root directory of the World of Warcraft installation.
    /// </summary>
    public string WowInstallRoot { get; init; } = string.Empty;

    /// <summary>
    /// Directory containing WoWDBDefs <c>.dbd</c> files.
    /// This is used by EF Core integrations that also register a file-system DBD provider.
    /// </summary>
    public string? DbdDefinitionsDirectory { get; init; }

    /// <summary>
    /// Directory where the DB2 manifest asset file (e.g. <c>manifest.json</c>) is located.
    /// Required when using the default DI registration (<c>AddCasc</c>) which wires up the local
    /// <see cref="FileSystemManifestProvider"/>.
    /// This setting is not required when a custom <see cref="IManifestProvider"/> is registered.
    /// </summary>
    public string ManifestDirectory { get; init; } = string.Empty;

    /// <summary>
    /// Name of the manifest asset file (default: <c>manifest.json</c>).
    /// </summary>
    public string ManifestAssetName { get; init; } = "manifest.json";

    /// <summary>
    /// Optional path to a TACT key file (e.g. <c>WoW.txt</c>) used to decrypt encrypted DB2 sections.
    /// When configured, the provider registers an <see cref="ITactKeyProvider"/> for WDC5 decoding.
    /// </summary>
    public string? TactKeyFilePath { get; init; }

    /// <summary>
    /// Controls how BLTE decoding behaves when an encrypted block is encountered but the corresponding TACT key
    /// is missing.
    /// When <see langword="false"/> (default), encrypted blocks are skipped and output is zero-filled.
    /// When <see langword="true"/>, decoding throws.
    /// </summary>
    public bool ThrowOnEncryptedBlockWithoutKey { get; init; } = false;

    /// <summary>
    /// The CASC product token (e.g. <c>wow</c>, <c>wowt</c>, <c>wow_classic</c>) that identifies
    /// which flavor to target. Default is <c>wow</c>.
    /// </summary>
    public string Product { get; init; } = "wow";

    private static string? GetString(DbConnectionStringBuilder csb, params ReadOnlySpan<string> keys)
    {
        foreach (var key in keys)
        {
            if (csb.TryGetValue(key, out var value) && value?.ToString()?.Trim() is { Length: > 0 } trimmed)
                return trimmed;
        }

        return null;
    }

    private static bool GetBool(DbConnectionStringBuilder csb, params ReadOnlySpan<string> keys)
    {
        return GetString(csb, keys) is { } value && bool.TryParse(value, out var b) && b;
    }
}
