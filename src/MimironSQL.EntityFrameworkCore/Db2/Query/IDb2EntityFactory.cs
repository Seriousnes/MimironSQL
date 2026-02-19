using System.Collections.Concurrent;
using System.Linq.Expressions;

namespace MimironSQL.EntityFrameworkCore.Db2.Query;

internal interface IDb2EntityFactory
{
    TEntity Create<TEntity>() where TEntity : class;
}

internal sealed class DefaultDb2EntityFactory : IDb2EntityFactory
{
    private static readonly ConcurrentDictionary<Type, Func<object>> Cache = [];

    public TEntity Create<TEntity>() where TEntity : class
    {
        var factory = Cache.GetOrAdd(typeof(TEntity), static t => CompileFactory<TEntity>());
        return (TEntity)factory();
    }

    private static Func<TEntity> CompileFactory<TEntity>() where TEntity : class
    {
        var type = typeof(TEntity);
        var ctor = type.GetConstructor(Type.EmptyTypes) ?? throw new NotSupportedException($"Entity type '{type.FullName}' must have a public parameterless constructor for materialization.");
        var newExpr = Expression.New(ctor);        
        return Expression.Lambda<Func<TEntity>>(newExpr).Compile();
    }
}
