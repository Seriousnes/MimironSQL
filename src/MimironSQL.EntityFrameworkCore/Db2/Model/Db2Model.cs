using System.Reflection;

namespace MimironSQL.EntityFrameworkCore.Db2.Model;

internal sealed class Db2Model
{
    private readonly IReadOnlyDictionary<Type, Lazy<Db2EntityType>> _entityTypes;
    private readonly IReadOnlyDictionary<(Type SourceClrType, MemberInfo NavigationMember), Lazy<Db2ReferenceNavigation>> _referenceNavigations;
    private readonly IReadOnlyDictionary<(Type SourceClrType, MemberInfo NavigationMember), Lazy<Db2CollectionNavigation>> _collectionNavigations;
    private readonly IReadOnlyDictionary<Type, IReadOnlyList<MemberInfo>> _autoIncludeNavigations;

    public Db2Model(
        IReadOnlyDictionary<Type, Db2EntityType> entityTypes,
        IReadOnlyDictionary<(Type SourceClrType, MemberInfo NavigationMember), Db2ReferenceNavigation> referenceNavigations,
        IReadOnlyDictionary<(Type SourceClrType, MemberInfo NavigationMember), Db2CollectionNavigation> collectionNavigations,
        IReadOnlyDictionary<Type, IReadOnlyList<MemberInfo>> autoIncludeNavigations)
        : this(
            entityTypes.ToDictionary(
                static kvp => kvp.Key,
                static kvp => new Lazy<Db2EntityType>(() => kvp.Value)),
            referenceNavigations.ToDictionary(
                static kvp => kvp.Key,
                static kvp => new Lazy<Db2ReferenceNavigation>(() => kvp.Value)),
            collectionNavigations.ToDictionary(
                static kvp => kvp.Key,
                static kvp => new Lazy<Db2CollectionNavigation>(() => kvp.Value)),
            autoIncludeNavigations)
    {
    }

    public Db2Model(
        IReadOnlyDictionary<Type, Lazy<Db2EntityType>> entityTypes,
        IReadOnlyDictionary<(Type SourceClrType, MemberInfo NavigationMember), Lazy<Db2ReferenceNavigation>> referenceNavigations,
        IReadOnlyDictionary<(Type SourceClrType, MemberInfo NavigationMember), Lazy<Db2CollectionNavigation>> collectionNavigations,
        IReadOnlyDictionary<Type, IReadOnlyList<MemberInfo>> autoIncludeNavigations)
    {
        _entityTypes = entityTypes;
        _referenceNavigations = referenceNavigations;
        _collectionNavigations = collectionNavigations;
        _autoIncludeNavigations = autoIncludeNavigations;
    }

    public Db2EntityType GetEntityType(Type clrType)
        => _entityTypes.TryGetValue(clrType, out var entityType)
            ? entityType.Value
            : throw new KeyNotFoundException($"No entity type registered for CLR type '{clrType.FullName}'.");

    public bool TryGetReferenceNavigation(Type sourceClrType, MemberInfo navigationMember, out Db2ReferenceNavigation navigation)
    {
        if (_referenceNavigations.TryGetValue((sourceClrType, navigationMember), out var lazy))
        {
            navigation = lazy.Value;
            return true;
        }

        navigation = null!;
        return false;
    }

    public bool TryGetCollectionNavigation(Type sourceClrType, MemberInfo navigationMember, out Db2CollectionNavigation navigation)
    {
        if (_collectionNavigations.TryGetValue((sourceClrType, navigationMember), out var lazy))
        {
            navigation = lazy.Value;
            return true;
        }

        navigation = null!;
        return false;
    }

    public IReadOnlyList<MemberInfo> GetAutoIncludeNavigations(Type sourceClrType)
        => _autoIncludeNavigations.TryGetValue(sourceClrType, out var members) ? members : [];
}
