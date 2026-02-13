using System.Reflection;

namespace MimironSQL.EntityFrameworkCore.Db2.Query;

/// <summary>
/// Async-local storage for include chains extracted during query preprocessing.
/// Includes are stored here during <see cref="Db2QueryTranslationPreprocessor"/> processing
/// and retrieved during <see cref="Db2ShapedQueryCompilingExpressionVisitor"/> compilation.
/// </summary>
internal static class Db2IncludeStorage
{
    /// <summary>
    /// Stored include chains with the root entity type.
    /// </summary>
    internal sealed record StoredIncludes(
        Type RootEntityType,
        IReadOnlyList<MemberInfo[]> IncludeChains,
        bool IgnoreAutoIncludes);

    private static readonly AsyncLocal<StoredIncludes?> Current = new();

    /// <summary>
    /// Stores include chains for the current async context.
    /// </summary>
    public static void Store(Type rootEntityType, IReadOnlyList<MemberInfo[]> includeChains, bool ignoreAutoIncludes)
    {
        Current.Value = new StoredIncludes(rootEntityType, includeChains, ignoreAutoIncludes);
    }

    /// <summary>
    /// Retrieves and clears include chains for the current async context.
    /// Returns null if no includes were stored.
    /// </summary>
    public static StoredIncludes? RetrieveAndClear()
    {
        var value = Current.Value;
        Current.Value = null;
        return value;
    }

    /// <summary>
    /// Clears any stored includes for the current async context.
    /// </summary>
    public static void Clear()
    {
        Current.Value = null;
    }
}
