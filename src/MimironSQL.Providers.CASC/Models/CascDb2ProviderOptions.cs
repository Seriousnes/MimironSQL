namespace MimironSQL.Providers;

/// <summary>
/// Options for configuring the CASC provider.
/// </summary>
public sealed record CascDb2ProviderOptions
{
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
}
