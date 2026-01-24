using System.Reflection;

namespace MimironSQL.Db2.Model;

public sealed class Db2Model(
    IReadOnlyDictionary<Type, Db2EntityType> entityTypes,
    IReadOnlyDictionary<(Type SourceClrType, MemberInfo NavigationMember), Db2ReferenceNavigation> referenceNavigations)
{
    private readonly IReadOnlyDictionary<Type, Db2EntityType> _entityTypes = entityTypes;
    private readonly IReadOnlyDictionary<(Type SourceClrType, MemberInfo NavigationMember), Db2ReferenceNavigation> _referenceNavigations = referenceNavigations;

    public bool TryGetEntityType(Type clrType, out Db2EntityType entityType)
        => _entityTypes.TryGetValue(clrType, out entityType!);

    public Db2EntityType GetEntityType(Type clrType)
        => _entityTypes.TryGetValue(clrType, out var entityType)
            ? entityType
            : throw new KeyNotFoundException($"No entity type registered for CLR type '{clrType.FullName}'.");

    public bool TryGetReferenceNavigation(Type sourceClrType, MemberInfo navigationMember, out Db2ReferenceNavigation navigation)
        => _referenceNavigations.TryGetValue((sourceClrType, navigationMember), out navigation!);
}
