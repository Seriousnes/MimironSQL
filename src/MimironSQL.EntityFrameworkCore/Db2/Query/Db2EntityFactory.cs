using System.Collections.Concurrent;
using System.Linq.Expressions;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Metadata;

namespace MimironSQL.Db2.Query;

internal interface IDb2EntityFactory
{
    object Create(Type entityClrType);

    T Create<T>();
}

internal sealed class ReflectionDb2EntityFactory : IDb2EntityFactory
{
    private static readonly ConcurrentDictionary<Type, Func<object>> FactoryCache = new();

    public object Create(Type entityClrType)
    {
        ArgumentNullException.ThrowIfNull(entityClrType);
        return FactoryCache.GetOrAdd(entityClrType, CreateFactory)();
    }

    public T Create<T>() => (T)Create(typeof(T));

    private static Func<object> CreateFactory(Type entityClrType)
    {
        var ctor = entityClrType.GetConstructor(Type.EmptyTypes)
            ?? throw new NotSupportedException($"Entity type {entityClrType.FullName} must have a public parameterless constructor for reflection-based materialization.");

        var body = Expression.New(ctor);
        return Expression.Lambda<Func<object>>(Expression.Convert(body, typeof(object))).Compile();
    }
}

internal sealed class EfLazyLoadingProxyDb2EntityFactory : IDb2EntityFactory
{
    private static readonly ConcurrentDictionary<Type, Func<EfLazyLoadingProxyDb2EntityFactory, object>> CreatorCache = new();

    private readonly DbContext _context;
    private readonly IServiceProvider _services;
    private readonly IDb2EntityFactory _fallback;

    public EfLazyLoadingProxyDb2EntityFactory(DbContext context, IDb2EntityFactory fallback)
    {
        _context = context ?? throw new ArgumentNullException(nameof(context));
        _services = context.GetInfrastructure();
        _fallback = fallback ?? throw new ArgumentNullException(nameof(fallback));
    }

    public object Create(Type entityClrType)
    {
        ArgumentNullException.ThrowIfNull(entityClrType);
        return CreatorCache.GetOrAdd(entityClrType, static t => f => f.CreateForType(t))(this);
    }

    public T Create<T>() => (T)Create(typeof(T));

    private object CreateForType(Type entityClrType)
    {
        var efEntityType = FindEfEntityType(entityClrType);
        if (efEntityType is null)
            return _fallback.Create(entityClrType);

        if (!TryCreateLazyLoadingProxy(efEntityType, out var proxy))
            return _fallback.Create(entityClrType);

        return proxy;
    }

    private IEntityType? FindEfEntityType(Type clrType)
    {
        var current = clrType;
        while (current is not null)
        {
            var et = _context.Model.FindEntityType(current);
            if (et is not null)
                return et;

            current = current.BaseType;
        }

        return null;
    }

    private bool TryCreateLazyLoadingProxy(IEntityType efEntityType, out object proxy)
    {
        proxy = null!;

        // Proxies are only available when Microsoft.EntityFrameworkCore.Proxies is enabled.
        var proxyFactoryType = ResolveTypeByFullName("Microsoft.EntityFrameworkCore.Proxies.Internal.IProxyFactory");
        if (proxyFactoryType is null)
            return false;

        var proxyFactory = _services.GetService(proxyFactoryType);
        if (proxyFactory is null)
            return false;

        try
        {
            // Prefer CreateLazyLoadingProxy when available.
            if (TryInvokeProxyFactory(proxyFactory, efEntityType, preferLazyLoading: true, out proxy))
                return true;

            if (TryInvokeProxyFactory(proxyFactory, efEntityType, preferLazyLoading: false, out proxy))
                return true;

            return false;
        }
        catch
        {
            proxy = null!;
            return false;
        }
    }

    private bool TryInvokeProxyFactory(object proxyFactory, IEntityType efEntityType, bool preferLazyLoading, out object proxy)
    {
        proxy = null!;

        var names = preferLazyLoading
            ? new[] { "CreateLazyLoadingProxy", "CreateProxy" }
            : new[] { "CreateProxy", "CreateLazyLoadingProxy" };

        var methods = proxyFactory.GetType().GetMethods();

        var lazyLoader = _services.GetService(typeof(ILazyLoader));

        for (var nameIndex = 0; nameIndex < names.Length; nameIndex++)
        {
            var methodName = names[nameIndex];

            for (var i = 0; i < methods.Length; i++)
            {
                var m = methods[i];
                if (!string.Equals(m.Name, methodName, StringComparison.Ordinal))
                    continue;

                var parameters = m.GetParameters();
                if (parameters.Length < 2)
                    continue;

                var args = new object?[parameters.Length];

                for (var pIndex = 0; pIndex < parameters.Length; pIndex++)
                {
                    var pType = parameters[pIndex].ParameterType;

                    if (pType == typeof(DbContext))
                    {
                        args[pIndex] = _context;
						continue;
					}

                    if (typeof(IEntityType).IsAssignableFrom(pType))
                    {
                        args[pIndex] = efEntityType;
                        continue;
                    }

                    if (pType == typeof(ILazyLoader))
                    {
                        args[pIndex] = lazyLoader;
						continue;
					}

                    if (pType == typeof(object[]))
                    {
                        args[pIndex] = Array.Empty<object>();
                        continue;
                    }

                    args[pIndex] = pType.IsValueType ? Activator.CreateInstance(pType) : null;
                }

                proxy = m.Invoke(proxyFactory, args)!;
                return proxy is not null;
            }
        }

        return false;
    }

    private static Type? ResolveTypeByFullName(string fullName)
    {
        foreach (var a in AppDomain.CurrentDomain.GetAssemblies())
        {
            var t = a.GetType(fullName, throwOnError: false, ignoreCase: false);
            if (t is not null)
                return t;
        }

        return null;
    }
}
