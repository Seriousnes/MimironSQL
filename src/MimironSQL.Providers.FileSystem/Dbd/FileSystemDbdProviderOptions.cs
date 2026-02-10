namespace MimironSQL.Providers;

/// <summary>
/// Options for <see cref="FileSystemDbdProvider"/>.
/// </summary>
/// <param name="DefinitionsDirectory">Directory containing <c>.dbd</c> definition files.</param>
public sealed record FileSystemDbdProviderOptions(
    string DefinitionsDirectory);
