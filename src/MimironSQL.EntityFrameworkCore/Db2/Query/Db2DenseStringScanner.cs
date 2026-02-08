using System.Runtime.CompilerServices;
using System.Text;

namespace MimironSQL.EntityFrameworkCore.Db2.Query;

internal enum Db2StringMatchKind
{
    Contains = 0,
    StartsWith,
    EndsWith,
}

internal static class Db2DenseStringScanner
{
    private const int DefaultCacheCapacity = 256;
    private static ConditionalWeakTable<object, PerFileCache> Cache = new();

    internal static void ClearCacheForTesting()
        => Cache = new ConditionalWeakTable<object, PerFileCache>();

    public static HashSet<int> FindStartOffsetsCached(object fileKey, ReadOnlySpan<byte> bytes, string needle, Db2StringMatchKind kind)
    {
        if (string.IsNullOrEmpty(needle))
            return [];

        var perFile = Cache.GetValue(fileKey, static _ => new PerFileCache(DefaultCacheCapacity));

        if (perFile.TryGetValue(needle, kind, out var existing))
            return existing;

        var created = FindStartOffsets(bytes, needle, kind);
        return perFile.AddOrGetExisting(needle, kind, created);
    }

    public static HashSet<int> FindStartOffsets(ReadOnlySpan<byte> bytes, string needle, Db2StringMatchKind kind)
    {
        if (string.IsNullOrEmpty(needle))
            return [];

        var needleBytes = Encoding.UTF8.GetBytes(needle);
        if (needleBytes is { Length: 0 })
            return [];

        HashSet<int> starts = [];

        var idx = 0;
        while (idx < bytes.Length)
        {
            var found = bytes[idx..].IndexOf(needleBytes);
            if (found < 0)
                break;

            var matchIndex = idx + found;

            var start = matchIndex;
            while (start > 0 && bytes[start - 1] != 0)
                start--;

            var add = kind switch
            {
                Db2StringMatchKind.Contains => true,
                Db2StringMatchKind.StartsWith => start == matchIndex,
                Db2StringMatchKind.EndsWith => matchIndex + needleBytes.Length < bytes.Length && bytes[matchIndex + needleBytes.Length] == 0,
                _ => false,
            };

            if (add)
                starts.Add(start);

            idx = matchIndex + 1;
        }

        return starts;
    }

    private sealed class PerFileCache(int capacity)
    {
        private readonly object _gate = new();
        private readonly int _capacity = capacity;
        private readonly LinkedList<CacheKey> _lru = new();
        private readonly Dictionary<CacheKey, (LinkedListNode<CacheKey> Node, HashSet<int> Value)> _entries = new(CacheKeyComparer.Instance);

        public bool TryGetValue(string needle, Db2StringMatchKind kind, out HashSet<int> value)
        {
            var key = new CacheKey(needle, kind);

            lock (_gate)
            {
                if (_entries.TryGetValue(key, out var existing))
                {
                    _lru.Remove(existing.Node);
                    _lru.AddFirst(existing.Node);
                    value = existing.Value;
                    return true;
                }

                value = null!;
                return false;
            }
        }

        public HashSet<int> AddOrGetExisting(string needle, Db2StringMatchKind kind, HashSet<int> created)
        {
            var key = new CacheKey(needle, kind);

            lock (_gate)
            {
                if (_entries.TryGetValue(key, out var existing))
                {
                    _lru.Remove(existing.Node);
                    _lru.AddFirst(existing.Node);
                    return existing.Value;
                }

                var node = _lru.AddFirst(key);
                _entries.Add(key, (node, created));

                if (_entries.Count > _capacity)
                {
                    var last = _lru.Last;
                    if (last is not null)
                    {
                        _lru.RemoveLast();
                        _entries.Remove(last.Value);
                    }
                }

                return created;
            }
        }
    }

    private readonly record struct CacheKey(string Needle, Db2StringMatchKind Kind);

    private sealed class CacheKeyComparer : IEqualityComparer<CacheKey>
    {
        public static CacheKeyComparer Instance { get; } = new();

        public bool Equals(CacheKey x, CacheKey y)
            => x.Kind == y.Kind && string.Equals(x.Needle, y.Needle, StringComparison.Ordinal);

        public int GetHashCode(CacheKey obj)
            => HashCode.Combine((int)obj.Kind, StringComparer.Ordinal.GetHashCode(obj.Needle));
    }
}
