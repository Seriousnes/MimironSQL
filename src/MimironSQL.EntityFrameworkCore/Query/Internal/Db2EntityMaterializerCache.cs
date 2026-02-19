using System.Collections.Concurrent;

using Microsoft.EntityFrameworkCore.Metadata;

using MimironSQL.EntityFrameworkCore.Model;
using MimironSQL.Formats;

namespace MimironSQL.EntityFrameworkCore.Query.Internal;

internal static class Db2EntityMaterializerCache
{
    private readonly record struct CacheKey(Type ClrType, Db2EntityType Db2EntityType);

    private static readonly ConcurrentDictionary<CacheKey, Delegate> Cache = new();

    public static Action<TEntity, IDb2File, RowHandle> GetOrCompile<TEntity>(IModel _efModel, Db2EntityType entityType)
        where TEntity : class
    {
        var key = new CacheKey(typeof(TEntity), entityType);
        var del = Cache.GetOrAdd(key, static k => Db2EntityMaterializer<TEntity>.CompileApply(k.Db2EntityType));
        return (Action<TEntity, IDb2File, RowHandle>)del;
    }
}
