using System;

namespace MimironSQL.Providers;

public interface ITactKeyProvider
{
    bool TryGetKey(ulong tactKeyLookup, out ReadOnlyMemory<byte> key);
}
