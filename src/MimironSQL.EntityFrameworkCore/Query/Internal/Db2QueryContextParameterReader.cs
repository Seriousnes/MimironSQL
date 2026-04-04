using System.Collections.Concurrent;
using System.Linq.Expressions;
using System.Reflection;

using Microsoft.EntityFrameworkCore.Query;

namespace MimironSQL.EntityFrameworkCore.Query.Internal;

internal static class Db2QueryContextParameterReader
{
    private sealed record ParameterStoresAccessor(Func<QueryContext, object?>[] Stores);

    private static readonly ConcurrentDictionary<Type, ParameterStoresAccessor> QueryContextParameterStoresAccessorCache = new();

    internal static int GetIntParameterValue(QueryContext queryContext, string parameterName)
    {
        ArgumentNullException.ThrowIfNull(queryContext);
        ArgumentException.ThrowIfNullOrWhiteSpace(parameterName);

        if (!TryGetQueryContextParameterValue(queryContext, parameterName, out var value))
        {
            throw new NotSupportedException(
                $"MimironDb2 could not read parameter '{parameterName}' from QueryContext (runtime type '{queryContext.GetType().FullName}') to evaluate a parameterized Take() limit.");
        }

        if (value is null)
        {
            throw new NotSupportedException(
                $"MimironDb2 parameter '{parameterName}' from QueryContext was null; cannot evaluate a parameterized Take() limit.");
        }

        try
        {
            return value switch
            {
                int i => i,
                byte b => b,
                sbyte sb => sb,
                short s => s,
                ushort us => us,
                long l => checked((int)l),
                ulong ul => checked((int)ul),
                uint ui => checked((int)ui),
                nint ni => checked((int)ni),
                nuint nui => checked((int)nui),
                _ when value is IConvertible => Convert.ToInt32(value, provider: null),
                _ => throw new NotSupportedException(
                    $"MimironDb2 Take() limit parameter '{parameterName}' had unsupported runtime type '{value.GetType().FullName}'.")
            };
        }
        catch (OverflowException ex)
        {
            throw new NotSupportedException(
                $"MimironDb2 Take() limit parameter '{parameterName}' value '{value}' could not be converted to int without overflow.",
                ex);
        }
    }

    private static bool TryGetQueryContextParameterValue(QueryContext queryContext, string parameterName, out object? value)
    {
        var accessor = QueryContextParameterStoresAccessorCache.GetOrAdd(
            queryContext.GetType(),
            static t => new ParameterStoresAccessor(BuildQueryContextParameterStoresAccessors(t).ToArray()));

        foreach (var storeAccessor in accessor.Stores)
        {
            var store = storeAccessor(queryContext);
            if (store is null)
            {
                continue;
            }

            if (TryGetValueFromStore(store, parameterName, out value))
            {
                return true;
            }
        }

        value = null;
        return false;
    }

    private static IEnumerable<Func<QueryContext, object?>> BuildQueryContextParameterStoresAccessors(Type queryContextRuntimeType)
    {
        const BindingFlags InstanceAnyVisibility = BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic;

        foreach (var member in EnumerateCandidateStoreMembers(queryContextRuntimeType, InstanceAnyVisibility))
        {
            yield return BuildMemberAccessor(member);
        }

        static IEnumerable<MemberInfo> EnumerateCandidateStoreMembers(Type t, BindingFlags flags)
        {
            for (var current = t; current is not null; current = current.BaseType)
            {
                foreach (var p in current.GetProperties(flags))
                {
                    if (p.GetIndexParameters().Length != 0)
                    {
                        continue;
                    }

                    if (LooksLikeStringKeyedDictionary(p.PropertyType))
                    {
                        yield return p;
                    }
                }

                foreach (var f in current.GetFields(flags))
                {
                    if (LooksLikeStringKeyedDictionary(f.FieldType))
                    {
                        yield return f;
                    }
                }
            }
        }

        static bool LooksLikeStringKeyedDictionary(Type candidate)
        {
            if (typeof(System.Collections.IDictionary).IsAssignableFrom(candidate))
            {
                return true;
            }

            if (!candidate.IsGenericType && candidate.GetInterfaces().Length == 0)
            {
                return false;
            }

            foreach (var i in candidate.GetInterfaces().Append(candidate))
            {
                if (!i.IsGenericType)
                {
                    continue;
                }

                var def = i.GetGenericTypeDefinition();
                if (def != typeof(IReadOnlyDictionary<,>) && def != typeof(IDictionary<,>))
                {
                    continue;
                }

                var args = i.GetGenericArguments();
                if (args.Length == 2 && args[0] == typeof(string))
                {
                    return true;
                }
            }

            return false;
        }

        static Func<QueryContext, object?> BuildMemberAccessor(MemberInfo member)
        {
            var qc = Expression.Parameter(typeof(QueryContext), "qc");
            var instance = Expression.Convert(qc, member.DeclaringType!);

            Expression access = member switch
            {
                PropertyInfo p => Expression.Property(instance, p),
                FieldInfo f => Expression.Field(instance, f),
                _ => Expression.Constant(null, typeof(object))
            };

            var body = Expression.Convert(access, typeof(object));
            return Expression.Lambda<Func<QueryContext, object?>>(body, qc).Compile();
        }
    }

    private static bool TryGetValueFromStore(object store, string parameterName, out object? value)
    {
        if (store is System.Collections.IDictionary dict)
        {
            if (!dict.Contains(parameterName))
            {
                value = null;
                return false;
            }

            value = dict[parameterName];
            return true;
        }

        var storeType = store.GetType();
        var tryGetValue = storeType
            .GetMethods(BindingFlags.Instance | BindingFlags.Public)
            .FirstOrDefault(m =>
                m.Name == "TryGetValue"
                && m.GetParameters().Length == 2
                && m.GetParameters()[0].ParameterType == typeof(string)
                && m.GetParameters()[1].ParameterType.IsByRef);

        if (tryGetValue is null)
        {
            value = null;
            return false;
        }

        var args = new object?[] { parameterName, null };
        var ok = (bool)tryGetValue.Invoke(store, args)!;
        value = args[1];
        return ok;
    }
}
