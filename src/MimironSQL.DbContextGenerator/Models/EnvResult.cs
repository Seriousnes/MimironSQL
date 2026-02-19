namespace MimironSQL.DbContextGenerator.Models;

internal readonly struct EnvResult(EnvResultKind kind, WowVersion? version, string? rawValue)
{
    /// <summary>
    /// Gets the kind of environment read result.
    /// </summary>
    public EnvResultKind Kind { get; } = kind;

    /// <summary>
    /// Gets the parsed WoW version when available.
    /// </summary>
    public WowVersion? Version { get; } = version;

    /// <summary>
    /// Gets the raw <c>WOW_VERSION</c> value when present.
    /// </summary>
    public string? RawValue { get; } = rawValue;

    /// <summary>
    /// Gets an <see cref="EnvResult"/> that represents a missing <c>.env</c> file.
    /// </summary>
    public static EnvResult Missing => new(EnvResultKind.MissingEnv, null, null);

    /// <summary>
    /// Gets an <see cref="EnvResult"/> that represents a missing <c>WOW_VERSION</c> key.
    /// </summary>
    public static EnvResult MissingWowVersion => new(EnvResultKind.MissingWowVersion, null, null);
}
