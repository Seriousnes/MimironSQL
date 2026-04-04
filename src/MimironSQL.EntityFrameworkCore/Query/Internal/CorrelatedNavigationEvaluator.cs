using System.Linq.Expressions;
using System.Reflection;
using System.Runtime.CompilerServices;

using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Query;

using MimironSQL.Formats;
using MimironSQL.Formats.Wdc5;
using MimironSQL.EntityFrameworkCore.Storage;
using MimironSQL.EntityFrameworkCore.Model;

namespace MimironSQL.EntityFrameworkCore.Query.Internal;

internal static class CorrelatedNavigationEvaluator
{
    internal static readonly MethodInfo CorrelatedAnyMethodInfo = typeof(CorrelatedNavigationEvaluator)
        .GetMethod(nameof(CorrelatedAny), BindingFlags.Public | BindingFlags.Static)!;

    internal static readonly MethodInfo CorrelatedCountMethodInfo = typeof(CorrelatedNavigationEvaluator)
        .GetMethod(nameof(CorrelatedCount), BindingFlags.Public | BindingFlags.Static)!;

    private readonly record struct CacheKey(Type InnerClrType, Type KeyType, string InnerKeyMemberName, Delegate? DependentPredicate, bool IsCount);

    private sealed class CorrelatedCounts
    {
        private readonly Dictionary<object, int> _counts = [];

        public int NullCount { get; private set; }

        public int GetCount(object? key)
        {
            if (key is null)
            {
                return NullCount;
            }

            return _counts.TryGetValue(key, out var count) ? count : 0;
        }

        public void Add(object? key)
        {
            if (key is null)
            {
                NullCount++;
                return;
            }

            _counts.TryGetValue(key, out var current);
            _counts[key] = current + 1;
        }
    }

    private static readonly ConditionalWeakTable<QueryContext, Dictionary<CacheKey, object>> Cache = [];

    private static readonly System.Collections.Concurrent.ConcurrentDictionary<(Type InnerClrType, Type KeyType, string MemberName), Delegate> KeyGetterCache = new();

    public static bool CorrelatedAny<TInner, TKey>(
        QueryContext queryContext,
        TKey outerKey,
        string innerKeyMemberName,
        Func<QueryContext, object, bool>? dependentPredicate)
        where TInner : class
    {
        return CorrelatedCount<TInner, TKey>(queryContext, outerKey, innerKeyMemberName, dependentPredicate) > 0;
    }

    public static int CorrelatedCount<TInner, TKey>(
        QueryContext queryContext,
        TKey outerKey,
        string innerKeyMemberName,
        Func<QueryContext, object, bool>? dependentPredicate)
        where TInner : class
    {
        ArgumentNullException.ThrowIfNull(queryContext);
        ArgumentException.ThrowIfNullOrWhiteSpace(innerKeyMemberName);

        var dict = Cache.GetOrCreateValue(queryContext);
        var key = new CacheKey(typeof(TInner), typeof(TKey), innerKeyMemberName, dependentPredicate, IsCount: true);

        if (!dict.TryGetValue(key, out var cached))
        {
            cached = BuildCounts<TInner, TKey>(queryContext, innerKeyMemberName, dependentPredicate);
            dict[key] = cached;
        }

        var counts = (CorrelatedCounts)cached;
        return counts.GetCount(outerKey);
    }

    private static CorrelatedCounts BuildCounts<TInner, TKey>(
        QueryContext queryContext,
        string innerKeyMemberName,
        Func<QueryContext, object, bool>? dependentPredicate)
        where TInner : class
    {
        var dbContext = queryContext.Context;

        var efEntityType = dbContext.Model.FindEntityType(typeof(TInner))
            ?? throw new NotSupportedException($"No EF entity type registered for '{typeof(TInner).FullName}'.");

        var tableName = efEntityType.GetTableName() ?? typeof(TInner).Name;

        var store = dbContext.GetService<IMimironDb2Store>();
        var (file, schema) = store.OpenTableWithSchema<RowHandle>(tableName);
        var format = dbContext.GetService<IDb2Format>() ?? new Wdc5Format();
        var layout = format.GetLayout(file);

        var modelBinding = dbContext.GetService<IDb2ModelBinding>().GetBinding();
        var entityType = modelBinding.GetEntityType(typeof(TInner));
        var materializer = new Db2EntityMaterializer<TInner>(modelBinding, entityType);

        var keyGetter = (Func<TInner, TKey>)KeyGetterCache.GetOrAdd((typeof(TInner), typeof(TKey), innerKeyMemberName), static k =>
        {
            var (innerClr, keyClr, member) = k;
            var prop = innerClr.GetProperty(member, BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic)
                ?? throw new NotSupportedException($"Member '{innerClr.FullName}.{member}' could not be resolved.");

            var innerParam = Expression.Parameter(innerClr, "e");
            var access = Expression.Property(innerParam, prop);
            var convert = Expression.Convert(access, keyClr);
            var lambda = Expression.Lambda(typeof(Func<,>).MakeGenericType(innerClr, keyClr), convert, innerParam);
            return lambda.Compile();
        });

        var counts = new CorrelatedCounts();

        foreach (var handle in file.EnumerateRowHandles())
        {
            var inner = materializer.Materialize(file, handle);

            if (dependentPredicate is not null && !dependentPredicate(queryContext, inner))
            {
                continue;
            }

            var k = keyGetter(inner);
            counts.Add(k);
        }

        return counts;
    }
}
