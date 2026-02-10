namespace MimironSQL.Providers;

/// <summary>
/// Options for <see cref="FileSystemDb2StreamProvider"/>.
/// </summary>
/// <param name="Db2DirectoryPath">Directory containing <c>.db2</c> files.</param>
public sealed record FileSystemDb2StreamProviderOptions(string Db2DirectoryPath);
