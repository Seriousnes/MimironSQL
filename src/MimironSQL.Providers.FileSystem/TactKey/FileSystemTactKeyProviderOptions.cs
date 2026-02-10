namespace MimironSQL.Providers;

/// <summary>
/// Options for <see cref="FileSystemTactKeyProvider"/>.
/// </summary>
/// <param name="KeyFilePath">Path to a TACT key file.</param>
public sealed record FileSystemTactKeyProviderOptions(string KeyFilePath);
