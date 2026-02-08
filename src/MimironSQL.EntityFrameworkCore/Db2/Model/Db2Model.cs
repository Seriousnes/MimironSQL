using System.Reflection;

namespace MimironSQL.EntityFrameworkCore.Db2.Model;

internal sealed class Db2Model(
    IReadOnlyDictionary<Type, Db2EntityType> entityTypes,
    IReadOnlyDictionary<(Type SourceClrType, MemberInfo NavigationMember), Db2ReferenceNavigation> referenceNavigations,
    IReadOnlyDictionary<(Type SourceClrType, MemberInfo NavigationMember), Db2CollectionNavigation> collectionNavigations,
    IReadOnlyDictionary<Type, IReadOnlyList<MemberInfo>> autoIncludeNavigations)
{
    private readonly IReadOnlyDictionary<Type, Db2EntityType> _entityTypes = entityTypes;
    private readonly IReadOnlyDictionary<(Type SourceClrType, MemberInfo NavigationMember), Db2ReferenceNavigation> _referenceNavigations = referenceNavigations;
    private readonly IReadOnlyDictionary<(Type SourceClrType, MemberInfo NavigationMember), Db2CollectionNavigation> _collectionNavigations = collectionNavigations;
    private readonly IReadOnlyDictionary<Type, IReadOnlyList<MemberInfo>> _autoIncludeNavigations = autoIncludeNavigations;

    public bool TryGetEntityType(Type clrType, out Db2EntityType entityType)
        => _entityTypes.TryGetValue(clrType, out entityType!);

    public Db2EntityType GetEntityType(Type clrType)
        => _entityTypes.TryGetValue(clrType, out var entityType)
            ? entityType
            : throw new KeyNotFoundException($"No entity type registered for CLR type '{clrType.FullName}'.");

    public bool TryGetReferenceNavigation(Type sourceClrType, MemberInfo navigationMember, out Db2ReferenceNavigation navigation)
        => _referenceNavigations.TryGetValue((sourceClrType, navigationMember), out navigation!);

    public bool TryGetCollectionNavigation(Type sourceClrType, MemberInfo navigationMember, out Db2CollectionNavigation navigation)
        => _collectionNavigations.TryGetValue((sourceClrType, navigationMember), out navigation!);

    public IReadOnlyList<MemberInfo> GetAutoIncludeNavigations(Type sourceClrType)
        => _autoIncludeNavigations.TryGetValue(sourceClrType, out var members) ? members : [];
}
