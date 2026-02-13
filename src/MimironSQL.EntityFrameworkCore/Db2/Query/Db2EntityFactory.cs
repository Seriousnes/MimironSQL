using System.Collections.Concurrent;
using System.Linq.Expressions;
using System.Reflection;
using System.Threading;

using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Metadata;

namespace MimironSQL.EntityFrameworkCore.Db2.Query;

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

internal sealed class EfLazyLoadingProxyDb2EntityFactory(DbContext context, IDb2EntityFactory fallback) : IDb2EntityFactory
{
    private static readonly ConcurrentDictionary<Type, Func<EfLazyLoadingProxyDb2EntityFactory, object>> CreatorCache = new();

    private static Type[] s_proxyFactoryServiceTypes = [];

    private static readonly ConcurrentDictionary<Type, ProxyFactoryInvokers> ProxyInvokersByServiceType = new();

    private readonly DbContext _context = context ?? throw new ArgumentNullException(nameof(context));
    private readonly IServiceProvider _services = context.GetInfrastructure();
    private readonly IDb2EntityFactory _fallback = fallback ?? throw new ArgumentNullException(nameof(fallback));

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
        var serviceTypes = GetProxyFactoryServiceTypes();
        if (serviceTypes.Count == 0)
            return false;

        object? proxyFactory = null;
        Type? proxyFactoryServiceType = null;

        // The service may be registered under an internal interface or a concrete type; probe candidates.
        for (var i = 0; i < serviceTypes.Count; i++)
        {
            var candidate = serviceTypes[i];
            var service = _services.GetService(candidate);
            if (service is null)
                continue;

            proxyFactory = service;
            proxyFactoryServiceType = candidate;
            break;
        }

        if (proxyFactory is null || proxyFactoryServiceType is null)
            return false;

        var lazyLoader = _services.GetService(typeof(ILazyLoader)) as ILazyLoader;

        var invokers = ProxyInvokersByServiceType.GetOrAdd(proxyFactoryServiceType, static t => ProxyFactoryInvokers.Build(t));

        try
        {
            // Prefer CreateLazyLoadingProxy when available.
            if (invokers.TryCreate(proxyFactory, _context, efEntityType, lazyLoader, preferLazyLoading: true, out proxy))
                return true;

            if (invokers.TryCreate(proxyFactory, _context, efEntityType, lazyLoader, preferLazyLoading: false, out proxy))
                return true;

            return false;
        }
        catch
        {
            proxy = null!;
            return false;
        }
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

    private static IReadOnlyList<Type> ResolveProxyFactoryServiceTypes()
    {
        // Prefer the historical internal full name (fast path).
        var byFullName = ResolveTypeByFullName("Microsoft.EntityFrameworkCore.Proxies.Internal.IProxyFactory");
        if (byFullName is not null)
            return [byFullName];

        // Fall back to a name+shape search to tolerate internal namespace moves across EF Core versions.
        // Return all plausible candidates; we will probe IServiceProvider at runtime.
        List<Type>? candidates = null;

        foreach (var assembly in AppDomain.CurrentDomain.GetAssemblies())
        {
            Type[] types;
            try
            {
                types = assembly.GetTypes();
            }
            catch (ReflectionTypeLoadException ex)
            {
                types = ex.Types.Where(static t => t is not null).Select(static t => t!).ToArray();
            }
            catch
            {
                continue;
            }

            for (var i = 0; i < types.Length; i++)
            {
                var t = types[i];
                // EF Proxies typically registers an internal interface service; some versions may use different internals.
                if (!string.Equals(t.Name, "IProxyFactory", StringComparison.Ordinal)
                    && !string.Equals(t.Name, "ProxyFactory", StringComparison.Ordinal))
                    continue;

                // Ensure it looks like EF Proxies' proxy factory.
                if (t.GetMethods().Any(static m =>
                        string.Equals(m.Name, "CreateLazyLoadingProxy", StringComparison.Ordinal)
                        || string.Equals(m.Name, "CreateProxy", StringComparison.Ordinal)))
                {
                    candidates ??= [];
                    candidates.Add(t);
                }
            }
        }

        return candidates ?? (IReadOnlyList<Type>)Array.Empty<Type>();
    }

    private static IReadOnlyList<Type> GetProxyFactoryServiceTypes()
    {
        var cached = Volatile.Read(ref s_proxyFactoryServiceTypes);
        if (cached.Length != 0)
            return cached;

        var resolved = ResolveProxyFactoryServiceTypes();

        // Important: do NOT permanently cache an empty result.
        // Test runners can load assemblies lazily; proxy types may appear later.
        if (resolved.Count == 0)
            return resolved;

        var resolvedArray = resolved as Type[] ?? resolved.ToArray();
        Interlocked.CompareExchange(ref s_proxyFactoryServiceTypes, resolvedArray, cached);
        return Volatile.Read(ref s_proxyFactoryServiceTypes);
    }

    private sealed class ProxyFactoryInvokers(
        IReadOnlyList<Func<object, DbContext, IEntityType, ILazyLoader?, object?>> lazyLoading,
        IReadOnlyList<Func<object, DbContext, IEntityType, ILazyLoader?, object?>> nonLazyLoading)
    {
        private readonly IReadOnlyList<Func<object, DbContext, IEntityType, ILazyLoader?, object?>> _lazyLoading = lazyLoading;
        private readonly IReadOnlyList<Func<object, DbContext, IEntityType, ILazyLoader?, object?>> _nonLazyLoading = nonLazyLoading;

        public static ProxyFactoryInvokers Build(Type? proxyFactoryType)
        {
            if (proxyFactoryType is null)
                return new ProxyFactoryInvokers([], []);

            var lazy = BuildInvokers(proxyFactoryType, methodName: "CreateLazyLoadingProxy");
            var create = BuildInvokers(proxyFactoryType, methodName: "CreateProxy");

            return new ProxyFactoryInvokers(lazyLoading: lazy, nonLazyLoading: create);
        }

        public bool TryCreate(
            object proxyFactory,
            DbContext context,
            IEntityType entityType,
            ILazyLoader? lazyLoader,
            bool preferLazyLoading,
            out object proxy)
        {
            proxy = null!;

            var first = preferLazyLoading ? _lazyLoading : _nonLazyLoading;
            var second = preferLazyLoading ? _nonLazyLoading : _lazyLoading;

            if (TryInvokeList(first, proxyFactory, context, entityType, lazyLoader, out proxy))
                return true;

            if (TryInvokeList(second, proxyFactory, context, entityType, lazyLoader, out proxy))
                return true;

            return false;
        }

        private static bool TryInvokeList(
            IReadOnlyList<Func<object, DbContext, IEntityType, ILazyLoader?, object?>> invokers,
            object proxyFactory,
            DbContext context,
            IEntityType entityType,
            ILazyLoader? lazyLoader,
            out object proxy)
        {
            for (var i = 0; i < invokers.Count; i++)
            {
                var created = invokers[i](proxyFactory, context, entityType, lazyLoader);
                if (created is null)
                    continue;

                proxy = created;
                return true;
            }

            proxy = null!;
            return false;
        }

        private static IReadOnlyList<Func<object, DbContext, IEntityType, ILazyLoader?, object?>> BuildInvokers(Type proxyFactoryType, string methodName)
        {
            var methods = proxyFactoryType.GetMethods()
                .Where(m => string.Equals(m.Name, methodName, StringComparison.Ordinal))
                .ToArray();

            if (methods.Length == 0)
                return [];

            var result = new List<Func<object, DbContext, IEntityType, ILazyLoader?, object?>>(methods.Length);

            for (var i = 0; i < methods.Length; i++)
            {
                var method = methods[i];
                if (method.ReturnType == typeof(void))
                    continue;

                var parameters = method.GetParameters();
                if (parameters.Length < 2)
                    continue;

                var proxyFactoryParam = Expression.Parameter(typeof(object), "proxyFactory");
                var contextParam = Expression.Parameter(typeof(DbContext), "context");
                var entityTypeParam = Expression.Parameter(typeof(IEntityType), "entityType");
                var lazyLoaderParam = Expression.Parameter(typeof(ILazyLoader), "lazyLoader");

                var args = new Expression[parameters.Length];
                for (var p = 0; p < parameters.Length; p++)
                {
                    var pType = parameters[p].ParameterType;

                    if (pType == typeof(DbContext))
                    {
                        args[p] = contextParam;
                        continue;
                    }

                    if (typeof(IEntityType).IsAssignableFrom(pType))
                    {
                        args[p] = Expression.Convert(entityTypeParam, pType);
                        continue;
                    }

                    if (pType == typeof(ILazyLoader))
                    {
                        args[p] = lazyLoaderParam;
                        continue;
                    }

                    if (pType == typeof(object[]))
                    {
                        args[p] = Expression.Constant(Array.Empty<object>(), typeof(object[]));
                        continue;
                    }

                    args[p] = Expression.Default(pType);
                }

                var call = Expression.Call(Expression.Convert(proxyFactoryParam, proxyFactoryType), method, args);
                var box = Expression.Convert(call, typeof(object));

                var lambda = Expression.Lambda<Func<object, DbContext, IEntityType, ILazyLoader?, object?>>(
                    box,
                    proxyFactoryParam,
                    contextParam,
                    entityTypeParam,
                    lazyLoaderParam);

                result.Add(lambda.Compile());
            }

            return result;
        }
    }
}
