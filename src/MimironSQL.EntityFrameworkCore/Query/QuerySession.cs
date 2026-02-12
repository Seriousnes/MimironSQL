using System.Linq.Expressions;
using System.Reflection;

using Microsoft.EntityFrameworkCore;

using MimironSQL.EntityFrameworkCore.Db2.Model;
using MimironSQL.EntityFrameworkCore.Db2.Query;
using MimironSQL.EntityFrameworkCore.Db2.Schema;
using MimironSQL.EntityFrameworkCore.Storage;
using MimironSQL.Formats;

namespace MimironSQL.EntityFrameworkCore.Query;

internal sealed class QuerySession<TRow>
    : IDisposable
    where TRow : struct, IRowHandle
{
    private readonly DbContext _context;
    private readonly IMimironDb2Store _store;
    private readonly Db2Model _model;

    private readonly Dictionary<string, (IDb2File<TRow> File, Db2TableSchema Schema)> _tables = new(StringComparer.OrdinalIgnoreCase);

    public QuerySession(DbContext context, IMimironDb2Store store, Db2Model model)
    {
        ArgumentNullException.ThrowIfNull(context);
        ArgumentNullException.ThrowIfNull(store);
        ArgumentNullException.ThrowIfNull(model);

        _context = context;
        _store = store;
        _model = model;
    }

    public void Warm(Expression query, Type rootEntityClrType)
    {
        ArgumentNullException.ThrowIfNull(query);
        ArgumentNullException.ThrowIfNull(rootEntityClrType);

        var pipeline = Db2QueryPipeline.Parse(query);

        var includeChains = pipeline.Operations
            .OfType<Db2IncludeOperation>()
            .Where(static op => op.Members.Count != 0)
            .Select(static op => op.Members.ToArray())
            .ToList();

        if (!pipeline.IgnoreAutoIncludes)
            includeChains = ExpandIncludesWithAutoIncludes(rootEntityClrType, includeChains);

        var types = new HashSet<Type> { rootEntityClrType };

        for (var i = 0; i < includeChains.Count; i++)
        {
            var current = rootEntityClrType;
            var chain = includeChains[i];

            for (var j = 0; j < chain.Length; j++)
            {
                if (!TryGetNavigationTargetType(chain[j], out var next))
                    break;

                types.Add(next);
                current = next;
            }
        }

        var tableNames = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        foreach (var clrType in types)
            tableNames.Add(ResolveEfTableName(clrType));

        foreach (var name in tableNames)
            _ = Resolve(name);
    }

    public (IDb2File<TRow> File, Db2TableSchema Schema) Resolve(string tableName)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(tableName);

        if (_tables.TryGetValue(tableName, out var existing))
            return existing;

        var opened = _store.OpenTableWithSchema<TRow>(tableName);
        _tables[tableName] = opened;
        return opened;
    }

    public void Dispose()
    {
        foreach (var (_, (file, _)) in _tables)
            (file as IDisposable)?.Dispose();

        _tables.Clear();
    }

    private string ResolveEfTableName(Type entityClrType)
    {
        var efEntityType = _context.Model.FindEntityType(entityClrType)
            ?? throw new NotSupportedException($"Entity type '{entityClrType.FullName}' is not part of the EF model.");

        return efEntityType.GetTableName() ?? entityClrType.Name;
    }

    private List<MemberInfo[]> ExpandIncludesWithAutoIncludes(Type rootEntityClrType, List<MemberInfo[]> explicitChains)
    {
        var chains = explicitChains.ToList();

        foreach (var member in _model.GetAutoIncludeNavigations(rootEntityClrType))
        {
            if (chains.Any(c => c.Length == 1 && c[0] == member))
                continue;

            chains.Add([member]);
        }

        return ExpandAutoIncludeChains(rootEntityClrType, chains);
    }

    private List<MemberInfo[]> ExpandAutoIncludeChains(Type rootEntityClrType, List<MemberInfo[]> startingChains)
    {
        var seen = new HashSet<string>(StringComparer.Ordinal);
        var queue = new Queue<MemberInfo[]>(startingChains);
        var results = new List<MemberInfo[]>(startingChains.Count);

        while (queue.TryDequeue(out var chain))
        {
            var key = BuildChainKey(chain);
            if (!seen.Add(key))
                continue;

            results.Add(chain);

            if (!TryGetLeafType(rootEntityClrType, chain, out var leafType))
                continue;

            var nextMembers = _model.GetAutoIncludeNavigations(leafType);
            if (nextMembers.Count == 0)
                continue;

            for (var i = 0; i < nextMembers.Count; i++)
            {
                var next = nextMembers[i];
                if (chain.Contains(next))
                    continue;

                var appended = new MemberInfo[chain.Length + 1];
                Array.Copy(chain, appended, chain.Length);
                appended[^1] = next;
                queue.Enqueue(appended);
            }
        }

        return results;
    }

    private static bool TryGetLeafType(Type rootEntityClrType, MemberInfo[] chain, out Type leafType)
    {
        leafType = rootEntityClrType;

        for (var i = 0; i < chain.Length; i++)
        {
            if (!TryGetNavigationTargetType(chain[i], out var next))
                return false;

            leafType = next;
        }

        return true;
    }

    private static bool TryGetNavigationTargetType(MemberInfo member, out Type targetClrType)
    {
        var memberType = member switch
        {
            PropertyInfo p => p.PropertyType,
            FieldInfo f => f.FieldType,
            _ => throw new InvalidOperationException($"Unexpected member type: {member.GetType().FullName}"),
        };

        if (memberType == typeof(string))
        {
            targetClrType = memberType;
            return true;
        }

        if (memberType.IsArray)
        {
            targetClrType = memberType.GetElementType()!;
            return true;
        }

        if (TryGetEnumerableElementType(memberType, out var elementType))
        {
            targetClrType = elementType;
            return true;
        }

        targetClrType = memberType;
        return true;
    }

    private static bool TryGetEnumerableElementType(Type sequenceType, out Type elementType)
    {
        elementType = null!;

        if (sequenceType == typeof(string))
            return false;

        if (sequenceType.IsGenericType && sequenceType.GetGenericTypeDefinition() == typeof(IEnumerable<>))
        {
            elementType = sequenceType.GetGenericArguments()[0];
            return true;
        }

        foreach (var i in sequenceType.GetInterfaces())
        {
            if (!i.IsGenericType)
                continue;

            if (i.GetGenericTypeDefinition() != typeof(IEnumerable<>))
                continue;

            elementType = i.GetGenericArguments()[0];
            return true;
        }

        return false;
    }

    private static string BuildChainKey(MemberInfo[] chain)
    {
        if (chain.Length == 1)
        {
            var m0 = chain[0];
            return (m0.DeclaringType?.FullName ?? "<null>") + "." + m0.Name;
        }

        return string.Join(
            "->",
            chain.Select(static m => (m.DeclaringType?.FullName ?? "<null>") + "." + m.Name));
    }
}
