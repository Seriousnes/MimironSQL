using System.Collections.Concurrent;

namespace MimironSQL.Providers;

/// <summary>
/// Composes multiple <see cref="ITactKeyProvider"/> instances into a single provider.
/// </summary>
public sealed class CompositeTactKeyProvider : ITactKeyProvider
{
    private readonly ITactKeyProvider[] _providers;
    private readonly ConcurrentDictionary<ulong, byte[]> _cache = new();

    /// <summary>
    /// Creates a composite provider from one or more underlying providers.
    /// </summary>
    /// <param name="providers">The ordered providers to consult for key resolution.</param>
    public CompositeTactKeyProvider(IEnumerable<ITactKeyProvider> providers)
    {
        ArgumentNullException.ThrowIfNull(providers);
        _providers = providers.Where(static p => p is not null).ToArray();

        if (_providers.Length == 0)
        {
            throw new ArgumentException("At least one provider is required.", nameof(providers));
        }
    }

    /// <inheritdoc />
    public bool TryGetKey(ulong tactKeyLookup, out ReadOnlyMemory<byte> key)
    {
        if (_cache.TryGetValue(tactKeyLookup, out var cached))
        {
            key = cached;
            return true;
        }

        foreach (var provider in _providers)
        {
            if (!provider.TryGetKey(tactKeyLookup, out key))
            {
                continue;
            }

            if (!key.IsEmpty)
            {
                _cache.TryAdd(tactKeyLookup, key.ToArray());
            }

            return true;
        }

        key = default;
        return false;
    }
}
