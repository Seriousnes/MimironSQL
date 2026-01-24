using System.Reflection;

using MimironSQL.Db2.Query;

namespace MimironSQL.Db2.Model;

public sealed class Db2ModelBuilder
{
    private readonly Dictionary<Type, Db2EntityTypeMetadata> _entityTypes = [];

    public Db2EntityTypeBuilder<T> Entity<T>()
        => Entity(typeof(T)) is { } metadata
            ? new Db2EntityTypeBuilder<T>(this, metadata)
            : throw new InvalidOperationException("Unable to register entity type.");

    internal Db2EntityTypeMetadata Entity(Type clrType)
    {
        if (_entityTypes.TryGetValue(clrType, out var existing))
            return existing;

        var created = new Db2EntityTypeMetadata(clrType)
        {
            TableName = clrType.Name,
        };

        _entityTypes.Add(clrType, created);
        return created;
    }

    internal void ApplyTablePropertyConventions(Type contextType)
    {
        foreach (var p in contextType.GetProperties(BindingFlags.Instance | BindingFlags.Public))
        {
            if (p.PropertyType is not { IsGenericType: true } pt)
                continue;

            if (pt.GetGenericTypeDefinition() != typeof(Db2Table<>))
                continue;

            var entityType = pt.GetGenericArguments()[0];
            Entity(entityType);
        }
    }

    internal Db2Model Build()
    {
        var built = new Dictionary<Type, Db2EntityType>(_entityTypes.Count);

        var navigations = new Dictionary<(Type SourceClrType, MemberInfo NavigationMember), Db2ReferenceNavigation>();

        foreach (var (clrType, m) in _entityTypes)
        {
            var tableName = m.TableName ?? clrType.Name;
            built.Add(clrType, new Db2EntityType(clrType, tableName));

            foreach (var nav in m.Navigations)
            {
                if (nav.Kind is not { } kind)
                    throw new NotSupportedException($"Navigation '{nav.NavigationMember.Name}' on '{clrType.FullName}' must be configured (e.g., WithSharedPrimaryKey).");

                if (kind == Db2ReferenceNavigationKind.SharedPrimaryKeyOneToOne && (nav.SourceKeyMember is null || nav.TargetKeyMember is null))
                    throw new NotSupportedException($"Shared primary key navigation '{nav.NavigationMember.Name}' on '{clrType.FullName}' must specify both key selectors.");

                navigations[(clrType, nav.NavigationMember)] = new Db2ReferenceNavigation(
                    sourceClrType: clrType,
                    navigationMember: nav.NavigationMember,
                    targetClrType: nav.TargetClrType,
                    kind: kind,
                    sourceKeyMember: nav.SourceKeyMember!,
                    targetKeyMember: nav.TargetKeyMember!,
                    overridesSchema: nav.OverridesSchema);
            }
        }

        return new Db2Model(built, navigations);
    }
}
