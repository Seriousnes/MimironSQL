using System.Reflection;

using MimironSQL.Db2.Query;
using MimironSQL.Db2.Schema;

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

    internal void ApplySchemaNavigationConventions(Func<string, Db2TableSchema> schemaResolver)
    {
        ArgumentNullException.ThrowIfNull(schemaResolver);

        foreach (var (clrType, entityMetadata) in _entityTypes)
        {
            var tableName = entityMetadata.TableName ?? clrType.Name;
            var schema = schemaResolver(tableName);

            foreach (var f in schema.Fields)
            {
                if (f.ReferencedTableName is null)
                    continue;

                if (!f.Name.EndsWith("ID", StringComparison.OrdinalIgnoreCase))
                    continue;

                var navName = f.Name[..^2];

                var navMember = FindMember(clrType, navName);
                if (navMember is null)
                    continue;

                var fkMember = FindMember(clrType, f.Name);
                if (fkMember is null)
                    continue;

                var targetClrType = GetMemberType(navMember);
                if (targetClrType.IsValueType)
                    continue;

                var targetMetadata = Entity(targetClrType);

                if (!targetMetadata.TableNameWasConfigured && targetMetadata.TableName == targetClrType.Name && targetMetadata.TableName != f.ReferencedTableName)
                    targetMetadata.TableName = f.ReferencedTableName;

                var existing = entityMetadata.Navigations.FirstOrDefault(n => n.NavigationMember == navMember);
                if (existing is null)
                {
                    var nav = new Db2NavigationMetadata(navMember, targetClrType)
                    {
                        Kind = Db2ReferenceNavigationKind.ForeignKeyToPrimaryKey,
                        SourceKeyMember = fkMember,
                    };

                    entityMetadata.Navigations.Add(nav);
                    continue;
                }

                if (existing.OverridesSchema)
                    continue;

                if (existing is not { Kind: Db2ReferenceNavigationKind.ForeignKeyToPrimaryKey })
                    throw new NotSupportedException($"Navigation '{clrType.FullName}.{navName}' conflicts with schema FK '{f.Name}'. Use OverridesSchema() on the model configuration to override schema resolution.");

                if (existing.SourceKeyMember is not null && existing.SourceKeyMember != fkMember)
                    throw new NotSupportedException($"Navigation '{clrType.FullName}.{navName}' has FK member '{existing.SourceKeyMember.Name}' but schema FK is '{f.Name}'. Use OverridesSchema() to override schema resolution.");

                existing.SourceKeyMember = fkMember;
            }
        }
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

    internal Db2Model Build(Func<string, Db2TableSchema> schemaResolver)
    {
        ArgumentNullException.ThrowIfNull(schemaResolver);

        var built = new Dictionary<Type, Db2EntityType>(_entityTypes.Count);

        var navigations = new Dictionary<(Type SourceClrType, MemberInfo NavigationMember), Db2ReferenceNavigation>();

        foreach (var (clrType, m) in _entityTypes)
        {
            var tableName = m.TableName ?? clrType.Name;

            var schema = schemaResolver(tableName);

            var pkMember = ResolvePrimaryKeyMember(m);
            built.Add(clrType, new Db2EntityType(clrType, tableName, schema, pkMember));

            foreach (var nav in m.Navigations)
            {
                if (nav.Kind is not { } kind)
                    throw new NotSupportedException($"Navigation '{nav.NavigationMember.Name}' on '{clrType.FullName}' must be configured (e.g., WithSharedPrimaryKey).");

                if (kind == Db2ReferenceNavigationKind.SharedPrimaryKeyOneToOne && (nav.SourceKeyMember is null || nav.TargetKeyMember is null))
                    throw new NotSupportedException($"Shared primary key navigation '{nav.NavigationMember.Name}' on '{clrType.FullName}' must specify both key selectors.");

                if (kind == Db2ReferenceNavigationKind.ForeignKeyToPrimaryKey && nav.SourceKeyMember is null)
                    throw new NotSupportedException($"FK navigation '{nav.NavigationMember.Name}' on '{clrType.FullName}' must specify a foreign key selector (WithForeignKey) or be provided by schema conventions.");

                if (kind == Db2ReferenceNavigationKind.ForeignKeyToPrimaryKey && nav.TargetKeyMember is null)
                    nav.TargetKeyMember = ResolvePrimaryKeyMember(Entity(nav.TargetClrType));

                if (kind == Db2ReferenceNavigationKind.ForeignKeyToPrimaryKey && nav.TargetKeyMember is null)
                    throw new NotSupportedException($"FK navigation '{nav.NavigationMember.Name}' on '{clrType.FullName}' must resolve a target primary key.");

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

    private static MemberInfo ResolvePrimaryKeyMember(Db2EntityTypeMetadata metadata)
    {
        if (metadata.PrimaryKeyMember is not null)
            return metadata.PrimaryKeyMember;

        var candidates = metadata.ClrType.GetMember("Id", BindingFlags.Instance | BindingFlags.Public);
        foreach (var m in candidates)
        {
            if (m is PropertyInfo or FieldInfo)
                return m;
        }

        throw new NotSupportedException($"Entity type '{metadata.ClrType.FullName}' has no key member. Configure a primary key in OnModelCreating (e.g., modelBuilder.Entity<{metadata.ClrType.Name}>().HasKey(x => x.Id)).");
    }

    private static MemberInfo? FindMember(Type type, string name)
    {
        var members = type.GetMember(name, BindingFlags.Instance | BindingFlags.Public);
        foreach (var m in members)
        {
            if (m is PropertyInfo or FieldInfo)
                return m;
        }

        return null;
    }

    private static Type GetMemberType(MemberInfo member)
        => member switch
        {
            PropertyInfo p => p.PropertyType,
            FieldInfo f => f.FieldType,
            _ => throw new InvalidOperationException($"Unexpected member type: {member.GetType().FullName}"),
        };
}
