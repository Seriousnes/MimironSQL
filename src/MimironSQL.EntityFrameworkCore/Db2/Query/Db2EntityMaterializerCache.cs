using System.Collections.Concurrent;
using System.Runtime.CompilerServices;

using Microsoft.EntityFrameworkCore.Metadata;

using MimironSQL.EntityFrameworkCore.Db2.Model;
using MimironSQL.EntityFrameworkCore.Db2.Schema;
using MimironSQL.Formats;

namespace MimironSQL.EntityFrameworkCore.Db2.Query;

internal static class Db2EntityMaterializerCache
{
    private sealed class ModelCache
    {
        public readonly ConcurrentDictionary<CacheKey, Delegate> ApplyCache = new();
    }

    private readonly record struct CacheKey(Type EntityClrType, int SchemaSignature);

    private static readonly ConditionalWeakTable<IModel, ModelCache> Caches = [];

    public static void Precompile(IModel efModel, Db2EntityType entityType)
    {
        ArgumentNullException.ThrowIfNull(efModel);
        ArgumentNullException.ThrowIfNull(entityType);

        var method = typeof(Db2EntityMaterializerCache)
            .GetMethod(nameof(PrecompileTyped), System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!
            .MakeGenericMethod(entityType.ClrType);

        method.Invoke(null, [efModel, entityType]);
    }

    private static void PrecompileTyped<TEntity>(IModel efModel, Db2EntityType entityType)
        where TEntity : class
        => GetOrCompile<TEntity>(efModel, entityType);

    public static Action<TEntity, IDb2File, RowHandle> GetOrCompile<TEntity>(IModel efModel, Db2EntityType entityType)
        where TEntity : class
    {
        ArgumentNullException.ThrowIfNull(efModel);
        ArgumentNullException.ThrowIfNull(entityType);

        var cache = Caches.GetValue(efModel, static _ => new ModelCache());
        var key = new CacheKey(typeof(TEntity), ComputeSchemaSignature(entityType.Schema));

        return (Action<TEntity, IDb2File, RowHandle>)cache.ApplyCache.GetOrAdd(
            key,
            static (_, et) => Db2EntityMaterializer<TEntity>.CompileApply(et),
            entityType);
    }

    private static int ComputeSchemaSignature(Db2TableSchema schema)
    {
        var hashCode = new HashCode();

        hashCode.Add(StringComparer.OrdinalIgnoreCase.GetHashCode(schema.TableName));
        hashCode.Add(schema.PhysicalColumnCount);

        if (schema.AllowedLayoutHashes is not null)
        {
            for (var i = 0; i < schema.AllowedLayoutHashes.Count; i++)
                hashCode.Add(schema.AllowedLayoutHashes[i]);
        }

        var fields = schema.Fields;
        hashCode.Add(fields.Count);

        for (var i = 0; i < fields.Count; i++)
        {
            var f = fields[i];
            hashCode.Add(StringComparer.OrdinalIgnoreCase.GetHashCode(f.Name));
            hashCode.Add((int)f.ValueType);
            hashCode.Add(f.ColumnStartIndex);
            hashCode.Add(f.ElementCount);
            hashCode.Add(f.IsVirtual);
            hashCode.Add(f.IsId);
            hashCode.Add(f.IsRelation);

            if (f.ReferencedTableName is not null)
                hashCode.Add(StringComparer.OrdinalIgnoreCase.GetHashCode(f.ReferencedTableName));
            else
                hashCode.Add(0);
        }

        return hashCode.ToHashCode();
    }
}
