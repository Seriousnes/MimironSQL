using System.Collections.Concurrent;

namespace MimironSQL.EntityFrameworkCore.Query.Internal;

internal sealed class Db2FkGroupingCache
{
    internal readonly record struct Key(string TableName, string LayoutHash, int ForeignKeyFieldIndex);

    private readonly ConcurrentDictionary<Key, Lazy<IReadOnlyDictionary<int, int[]>>> _cache = new();

    public IReadOnlyDictionary<int, int[]> GetOrBuild(Key key, Func<IReadOnlyDictionary<int, int[]>> factory)
    {
        ArgumentNullException.ThrowIfNull(factory);

        var lazy = _cache.GetOrAdd(key, k => new Lazy<IReadOnlyDictionary<int, int[]>>(() => factory(), LazyThreadSafetyMode.ExecutionAndPublication));
        return lazy.Value;
    }
}
