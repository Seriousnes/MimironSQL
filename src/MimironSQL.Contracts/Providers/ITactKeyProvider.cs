namespace MimironSQL.Providers;

/// <summary>
/// Resolves TACT encryption keys for encrypted DB2 sections.
/// </summary>
public interface ITactKeyProvider
{
    /// <summary>
    /// Attempts to resolve the key bytes for a TACT key lookup identifier.
    /// </summary>
    /// <param name="tactKeyLookup">The lookup identifier from the DB2 section header.</param>
    /// <param name="key">When successful, receives the key bytes.</param>
    /// <returns><see langword="true"/> if a key is available; otherwise <see langword="false"/>.</returns>
    bool TryGetKey(ulong tactKeyLookup, out ReadOnlyMemory<byte> key);
}
