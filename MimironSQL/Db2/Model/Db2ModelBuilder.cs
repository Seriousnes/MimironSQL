using System.Reflection;

using MimironSQL.Db2.Query;
using MimironSQL.Db2.Schema;
using MimironSQL.Extensions;

namespace MimironSQL.Db2.Model;

public sealed class Db2ModelBuilder
{
    private bool configurationsApplied = false;
    private readonly Dictionary<Type, Db2EntityTypeMetadata> _entityTypes = [];

    public Db2EntityTypeBuilder<T> Entity<T>()
        => Entity(typeof(T)) is { } metadata
            ? new Db2EntityTypeBuilder<T>(this, metadata)
            : throw new InvalidOperationException("Unable to register entity type.");

    public Db2ModelBuilder ApplyConfiguration<T>(IDb2EntityTypeConfiguration<T> configuration)
    {
        ArgumentNullException.ThrowIfNull(configuration);

        var builder = Entity<T>();
        configuration.Configure(builder);
        return this;
    }

    public Db2ModelBuilder ApplyConfigurationsFromAssembly(params Assembly[] assemblies)
    {
        if (configurationsApplied)
            throw new InvalidOperationException("ApplyConfigurationsFromAssembly can only be called once per model builder.");

        var configInterfaceType = typeof(IDb2EntityTypeConfiguration<>);
        var configurations = new List<(Type ConfigType, Type EntityType)>();

        foreach (var a in assemblies)
        {
            configurations.AddRange(a.GetTypes()
                .Where(t => !t.IsAbstract && !t.IsGenericTypeDefinition)
                .Select(t => (Type: t, Interface: t.GetInterfaces()
                    .FirstOrDefault(i => i.IsGenericType && i.GetGenericTypeDefinition() == configInterfaceType)))
                .Where(x => x.Interface is not null)
                .Select(x => (ConfigType: x.Type, EntityType: x.Interface!.GetGenericArguments()[0])));
        }

        var duplicates = configurations
            .GroupBy(x => x.EntityType)
            .Where(g => g.Count() > 1)
            .Select(g => new
            {
                EntityType = g.Key,
                ConfigTypes = g.Select(x => x.ConfigType).OrderBy(t => t.FullName, StringComparer.Ordinal).ToArray(),
            })
            .OrderBy(x => x.EntityType.FullName, StringComparer.Ordinal)
            .ToArray();

        if (duplicates is { Length: not 0 })
        {
            var message = string.Join(
                Environment.NewLine,
                duplicates.Select(d =>
                    $"Multiple entity type configurations found for '{d.EntityType.FullName}': {string.Join(", ", d.ConfigTypes.Select(t => t.FullName))}."));

            throw new InvalidOperationException(message);
        }

        var applyMethodDefinition = typeof(Db2ModelBuilder).GetMethod(nameof(ApplyConfiguration)) ?? throw new InvalidOperationException(
            $"Unable to locate '{nameof(ApplyConfiguration)}' on '{typeof(Db2ModelBuilder).FullName}'.");

        foreach (var (configType, entityType) in configurations.OrderBy(x => x.ConfigType.FullName, StringComparer.Ordinal))
        {
            try
            {
                if (Activator.CreateInstance(configType) is not { } config)
                    continue;

                var applyMethod = applyMethodDefinition.MakeGenericMethod(entityType);
                applyMethod.Invoke(this, [config]);
            }
            catch (Exception ex) when (ex is MissingMethodException or TargetInvocationException or MemberAccessException or TypeLoadException)
            {
                throw new InvalidOperationException(
                    $"Unable to instantiate configuration type '{configType.FullName}'. " +
                    $"Ensure the configuration class has a public parameterless constructor.",
                    ex);
            }
        }

        configurationsApplied = true;
        return this;
    }

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

        var pending = new Queue<Type>(_entityTypes.Keys);
        var visited = new HashSet<Type>();

        while (pending.TryDequeue(out var clrType))
        {
            if (!visited.Add(clrType))
                continue;

            if (!_entityTypes.TryGetValue(clrType, out var entityMetadata))
                continue;

            var tableName = entityMetadata.TableName ?? clrType.Name;
            var schema = schemaResolver(tableName);

            foreach (var f in schema.Fields
                .Where(f => f.ReferencedTableName is not null && f.Name.EndsWith("ID", StringComparison.OrdinalIgnoreCase)))
            {
                var navName = f.Name[..^2];

                var navMember = FindMember(clrType, navName);
                if (navMember is null)
                    continue;

                var fkMember = FindMember(clrType, f.Name);
                if (fkMember is null)
                    continue;

                var navMemberType = navMember.GetMemberType();
                if (navMemberType != typeof(string) && typeof(System.Collections.IEnumerable).IsAssignableFrom(navMemberType))
                {
                    // Schema conventions only apply to 1-hop reference navigations.
                    // Collection navigations are not yet supported here.
                    continue;
                }

                var targetClrType = navMemberType;
                if (targetClrType.IsValueType)
                    continue;

                if (!_entityTypes.TryGetValue(targetClrType, out var targetMetadata))
                {
                    targetMetadata = new Db2EntityTypeMetadata(targetClrType)
                    {
                        TableName = targetClrType.Name,
                    };

                    _entityTypes.Add(targetClrType, targetMetadata);
                    pending.Enqueue(targetClrType);
                }

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
        foreach (var p in contextType.GetProperties(BindingFlags.Instance | BindingFlags.Public)
            .Where(p => p.PropertyType is { IsGenericType: true } && p.PropertyType.GetGenericTypeDefinition() == typeof(Db2Table<>))
            .Select(p => (Property: p, EntityType: p.PropertyType.GetGenericArguments()[0])))
        {
            Entity(p.EntityType);
        }
    }

    internal Db2Model Build(Func<string, Db2TableSchema> schemaResolver)
    {
        ArgumentNullException.ThrowIfNull(schemaResolver);

        var built = new Dictionary<Type, Db2EntityType>(_entityTypes.Count);

        var navigations = new Dictionary<(Type SourceClrType, MemberInfo NavigationMember), Db2ReferenceNavigation>();
        var collectionNavigations = new Dictionary<(Type SourceClrType, MemberInfo NavigationMember), Db2CollectionNavigation>();

        foreach (var (clrType, m) in _entityTypes)
        {
            var tableName = m.TableName ?? clrType.Name;

            var schema = schemaResolver(tableName);

            var pkMember = ResolvePrimaryKeyMember(m);
            var pkFieldSchema = ResolveFieldSchema(schema, pkMember, $"primary key member '{pkMember.Name}' of entity '{clrType.FullName}'");

            built.Add(clrType, new Db2EntityType(clrType, tableName, schema, pkMember, pkFieldSchema));

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

                var targetMetadata = Entity(nav.TargetClrType);
                var targetTableName = targetMetadata.TableName ?? nav.TargetClrType.Name;
                var targetSchema = schemaResolver(targetTableName);

                var sourceKeyFieldSchema = ResolveFieldSchema(schema, nav.SourceKeyMember!, $"source key member '{nav.SourceKeyMember!.Name}' in navigation '{clrType.FullName}.{nav.NavigationMember.Name}'");
                var targetKeyFieldSchema = ResolveFieldSchema(targetSchema, nav.TargetKeyMember!, $"target key member '{nav.TargetKeyMember!.Name}' in navigation '{clrType.FullName}.{nav.NavigationMember.Name}'");

                navigations[(clrType, nav.NavigationMember)] = new Db2ReferenceNavigation(
                    sourceClrType: clrType,
                    navigationMember: nav.NavigationMember,
                    targetClrType: nav.TargetClrType,
                    kind: kind,
                    sourceKeyMember: nav.SourceKeyMember!,
                    targetKeyMember: nav.TargetKeyMember!,
                    sourceKeyFieldSchema: sourceKeyFieldSchema,
                    targetKeyFieldSchema: targetKeyFieldSchema,
                    overridesSchema: nav.OverridesSchema);
            }

            foreach (var nav in m.CollectionNavigations)
            {
                if (nav.Kind is not { } kind)
                    throw new NotSupportedException($"Collection navigation '{nav.NavigationMember.Name}' on '{clrType.FullName}' must be configured (e.g., WithForeignKeyArray). ");

                if (kind == Db2CollectionNavigationKind.ForeignKeyArrayToPrimaryKey)
                {
                    if (nav.SourceKeyCollectionMember is null)
                        throw new NotSupportedException($"Collection navigation '{nav.NavigationMember.Name}' on '{clrType.FullName}' must specify a source key collection member (WithForeignKeyArray). ");

                    var sourceKeyCollectionType = nav.SourceKeyCollectionMember.GetMemberType();
                    if (!IsIntEnumerableType(sourceKeyCollectionType))
                    {
                        throw new NotSupportedException(
                            $"Collection navigation '{clrType.FullName}.{nav.NavigationMember.Name}' expects an int key collection (e.g., int[] or ICollection<int>) but found '{sourceKeyCollectionType.FullName}'.");
                    }

                    var sourceKeyFieldSchema = ResolveFieldSchema(schema, nav.SourceKeyCollectionMember, $"source key member '{nav.SourceKeyCollectionMember.Name}' in collection navigation '{clrType.FullName}.{nav.NavigationMember.Name}'");

                    collectionNavigations[(clrType, nav.NavigationMember)] = new Db2CollectionNavigation(
                        sourceClrType: clrType,
                        navigationMember: nav.NavigationMember,
                        targetClrType: nav.TargetClrType,
                        kind: kind,
                        sourceKeyCollectionMember: nav.SourceKeyCollectionMember,
                        sourceKeyFieldSchema: sourceKeyFieldSchema,
                        dependentForeignKeyMember: null,
                        dependentForeignKeyFieldSchema: null,
                        principalKeyMember: null,
                        overridesSchema: nav.OverridesSchema);

                    continue;
                }

                if (kind == Db2CollectionNavigationKind.DependentForeignKeyToPrimaryKey)
                {
                    if (nav.DependentForeignKeyMember is null)
                        throw new NotSupportedException($"Collection navigation '{nav.NavigationMember.Name}' on '{clrType.FullName}' must specify a dependent foreign key selector (WithForeignKey). ");

                    var principalKeyMember = nav.PrincipalKeyMember ?? ResolvePrimaryKeyMember(m);
                    var principalKeyType = principalKeyMember.GetMemberType();
                    if (!principalKeyType.IsScalarType())
                        throw new NotSupportedException($"Principal key member '{clrType.FullName}.{principalKeyMember.Name}' must be a scalar type.");

                    var dependentMetadata = Entity(nav.TargetClrType);
                    var dependentTableName = dependentMetadata.TableName ?? nav.TargetClrType.Name;
                    var dependentSchema = schemaResolver(dependentTableName);

                    var dependentFkFieldSchema = ResolveFieldSchema(dependentSchema, nav.DependentForeignKeyMember, $"dependent FK member '{nav.DependentForeignKeyMember.Name}' in collection navigation '{clrType.FullName}.{nav.NavigationMember.Name}'");

                    collectionNavigations[(clrType, nav.NavigationMember)] = new Db2CollectionNavigation(
                        sourceClrType: clrType,
                        navigationMember: nav.NavigationMember,
                        targetClrType: nav.TargetClrType,
                        kind: kind,
                        sourceKeyCollectionMember: null,
                        sourceKeyFieldSchema: null,
                        dependentForeignKeyMember: nav.DependentForeignKeyMember,
                        dependentForeignKeyFieldSchema: dependentFkFieldSchema,
                        principalKeyMember: principalKeyMember,
                        overridesSchema: nav.OverridesSchema);

                    continue;
                }

                throw new NotSupportedException($"Collection navigation '{nav.NavigationMember.Name}' on '{clrType.FullName}' has unsupported kind '{kind}'.");
            }
        }

        return new Db2Model(built, navigations, collectionNavigations);
    }

    private static bool IsIntEnumerableType(Type type)
    {
        if (type == typeof(int[]))
            return true;

        if (type.IsGenericType && type.GetGenericTypeDefinition() == typeof(IEnumerable<>))
            return type.GetGenericArguments()[0] == typeof(int);

        foreach (var i in type.GetInterfaces())
        {
            if (!i.IsGenericType)
                continue;

            if (i.GetGenericTypeDefinition() != typeof(IEnumerable<>))
                continue;

            return i.GetGenericArguments()[0] == typeof(int);
        }

        return false;
    }

    private static Db2FieldSchema ResolveFieldSchema(Db2TableSchema schema, MemberInfo member, string context)
    {
        var memberName = member.Name;
        if (!schema.TryGetFieldCaseInsensitive(memberName, out var fieldSchema))
        {
            throw new NotSupportedException(
                $"Field '{memberName}' not found in schema for table '{schema.TableName}'. " +
                $"This field is required for {context}. " +
                $"Ensure the member name matches a field in the .dbd definition.");
        }

        return fieldSchema;
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

}
