using System.Reflection;
using System.ComponentModel.DataAnnotations.Schema;

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

        foreach (var f in clrType.GetFields(BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic))
        {
            if (f.GetCustomAttribute<ColumnAttribute>(inherit: false) is not null)
                throw new NotSupportedException($"Column mapping attributes are only supported on public properties. Field '{clrType.FullName}.{f.Name}' is not a valid target.");

            if (f.GetCustomAttribute<ForeignKeyAttribute>(inherit: false) is not null)
                throw new NotSupportedException($"Foreign key mapping attributes are only supported on public properties. Field '{clrType.FullName}.{f.Name}' is not a valid target.");
        }

        foreach (var p in clrType.GetProperties(BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic))
        {
            if (p.GetCustomAttribute<ColumnAttribute>(inherit: false) is null)
            {
                if (p.GetCustomAttribute<ForeignKeyAttribute>(inherit: false) is null)
                    continue;

                if (p.GetMethod is not { IsPublic: true })
                {
                    throw new NotSupportedException(
                        $"Foreign key mapping attributes are only supported on public properties. Property '{clrType.FullName}.{p.Name}' is not a valid target.");
                }

                continue;
            }

            if (p.GetMethod is not { IsPublic: true })
            {
                throw new NotSupportedException(
                    $"Column mapping attributes are only supported on public properties. Property '{clrType.FullName}.{p.Name}' is not a valid target.");
            }
        }

        var tableAttr = clrType.GetCustomAttribute<TableAttribute>(inherit: false);
        var created = new Db2EntityTypeMetadata(clrType)
        {
            TableName = tableAttr?.Name ?? clrType.Name,
            TableNameWasConfigured = tableAttr is not null,
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

    internal void ApplyAttributeNavigationConventions()
    {
        var pending = new Queue<Type>(_entityTypes.Keys);
        var visited = new HashSet<Type>();

        while (pending.TryDequeue(out var clrType))
        {
            if (!visited.Add(clrType))
                continue;

            if (!_entityTypes.TryGetValue(clrType, out var metadata))
                continue;

            var properties = clrType.GetProperties(BindingFlags.Instance | BindingFlags.Public);

            foreach (var p in properties)
            {
                if (p.GetCustomAttribute<ForeignKeyAttribute>(inherit: false) is not { } fkAttr)
                    continue;

                if (string.IsNullOrWhiteSpace(fkAttr.Name))
                    throw new NotSupportedException($"[ForeignKey] on '{clrType.FullName}.{p.Name}' must specify a property name.");

                var foreignKeyName = ParseSingleForeignKeyName(fkAttr.Name, clrType, p);
                var overridesSchema = p.GetCustomAttribute<OverridesSchemaAttribute>(inherit: false) is not null;

                // Case 1: [ForeignKey] placed on a scalar FK property, pointing to a reference navigation.
                if (IsScalarForeignKeyProperty(p))
                {
                    var navMember = FindMember(clrType, foreignKeyName)
                        ?? throw new NotSupportedException($"[ForeignKey] on '{clrType.FullName}.{p.Name}' references navigation '{foreignKeyName}', but no matching public property was found.");

                    if (navMember is not PropertyInfo navProperty)
                        throw new NotSupportedException($"[ForeignKey] on '{clrType.FullName}.{p.Name}' must reference a public property.");

                    if (navProperty.GetCustomAttribute<ForeignKeyAttribute>(inherit: false) is not null)
                    {
                        throw new NotSupportedException(
                            $"Foreign key mapping for navigation '{clrType.FullName}.{navProperty.Name}' cannot specify [ForeignKey] on both the navigation and the FK property.");
                    }

                    if (TryGetIEnumerableElementType(navProperty.PropertyType, out _))
                        throw new NotSupportedException($"[ForeignKey] on '{clrType.FullName}.{p.Name}' cannot target a collection navigation '{navProperty.Name}'.");

                    if (navProperty.PropertyType.IsValueType)
                        throw new NotSupportedException($"Navigation '{clrType.FullName}.{navProperty.Name}' must be a reference type.");

                    if (metadata.Navigations.Any(n => n.NavigationMember == navProperty) || metadata.CollectionNavigations.Any(n => n.NavigationMember == navProperty))
                    {
                        throw new NotSupportedException(
                            $"Navigation '{clrType.FullName}.{navProperty.Name}' cannot be configured multiple times (fluent configuration and/or [ForeignKey]).");
                    }

                    var nav = new Db2NavigationMetadata(navProperty, navProperty.PropertyType)
                    {
                        Kind = Db2ReferenceNavigationKind.ForeignKeyToPrimaryKey,
                        SourceKeyMember = p,
                        OverridesSchema = overridesSchema,
                    };

                    metadata.Navigations.Add(nav);
                    pending.Enqueue(navProperty.PropertyType);
                    Entity(navProperty.PropertyType);
                    continue;
                }

                // Case 2: [ForeignKey] placed on a navigation property.
                if (TryGetIEnumerableElementType(p.PropertyType, out var elementType))
                {
                    ArgumentNullException.ThrowIfNull(elementType);

                    if (elementType.IsValueType)
                        throw new NotSupportedException($"Collection navigation '{clrType.FullName}.{p.Name}' must target a reference type.");

                    if (metadata.Navigations.Any(n => n.NavigationMember == p) || metadata.CollectionNavigations.Any(n => n.NavigationMember == p))
                    {
                        throw new NotSupportedException(
                            $"Navigation '{clrType.FullName}.{p.Name}' cannot be configured multiple times (fluent configuration and/or [ForeignKey]).");
                    }

                    var dependentFkMember = FindMember(elementType, foreignKeyName)
                        ?? throw new NotSupportedException($"[ForeignKey] on '{clrType.FullName}.{p.Name}' references dependent FK '{elementType.FullName}.{foreignKeyName}', but no matching public property was found.");

                    var collectionNav = new Db2CollectionNavigationMetadata(p, elementType)
                    {
                        Kind = Db2CollectionNavigationKind.DependentForeignKeyToPrimaryKey,
                        DependentForeignKeyMember = dependentFkMember,
                        OverridesSchema = overridesSchema,
                    };

                    metadata.CollectionNavigations.Add(collectionNav);
                    pending.Enqueue(elementType);
                    Entity(elementType);
                    continue;
                }

                if (p.PropertyType.IsValueType)
                    throw new NotSupportedException($"Navigation '{clrType.FullName}.{p.Name}' must be a reference type.");

                if (metadata.Navigations.Any(n => n.NavigationMember == p) || metadata.CollectionNavigations.Any(n => n.NavigationMember == p))
                {
                    throw new NotSupportedException(
                        $"Navigation '{clrType.FullName}.{p.Name}' cannot be configured multiple times (fluent configuration and/or [ForeignKey]).");
                }

                var fkMember = FindMember(clrType, foreignKeyName)
                    ?? throw new NotSupportedException($"[ForeignKey] on '{clrType.FullName}.{p.Name}' references FK '{foreignKeyName}', but no matching public property was found.");

                var referenceNav = new Db2NavigationMetadata(p, p.PropertyType)
                {
                    Kind = Db2ReferenceNavigationKind.ForeignKeyToPrimaryKey,
                    SourceKeyMember = fkMember,
                    OverridesSchema = overridesSchema,
                };

                metadata.Navigations.Add(referenceNav);
                pending.Enqueue(p.PropertyType);
                Entity(p.PropertyType);
            }
        }
    }

    internal void ApplyTablePropertyConventions(Type contextType)
    {
        foreach (var (Property, EntityType) in contextType.GetProperties(BindingFlags.Instance | BindingFlags.Public)
            .Where(p => p.PropertyType is { IsGenericType: true } && p.PropertyType.GetGenericTypeDefinition() == typeof(Db2Table<>))
            .Select(p => (Property: p, EntityType: p.PropertyType.GetGenericArguments()[0])))
        {
            Entity(EntityType);
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
            if (HasAnyColumnMapping(m, pkMember))
            {
                throw new NotSupportedException(
                    $"Primary key member '{clrType.FullName}.{pkMember.Name}' cannot configure column mapping via [Column] or HasColumnName().");
            }

            var pkFieldSchema = ResolveFieldSchema(schema, m, pkMember, $"primary key member '{pkMember.Name}' of entity '{clrType.FullName}'");

            built.Add(clrType, new Db2EntityType(
                clrType,
                tableName,
                schema,
                pkMember,
                pkFieldSchema,
                new Dictionary<string, string>(m.ColumnNameMappings, StringComparer.Ordinal)));

            foreach (var nav in m.Navigations)
            {
                if (nav.Kind is not { } kind)
                    throw new NotSupportedException($"Navigation '{nav.NavigationMember.Name}' on '{clrType.FullName}' must be configured (e.g., WithSharedPrimaryKey).");

                switch (kind)
                {
                    case Db2ReferenceNavigationKind.SharedPrimaryKeyOneToOne when (nav.SourceKeyMember is null || nav.TargetKeyMember is null):
                        throw new NotSupportedException($"Shared primary key navigation '{nav.NavigationMember.Name}' on '{clrType.FullName}' must specify both key selectors.");
                    case Db2ReferenceNavigationKind.ForeignKeyToPrimaryKey when nav.SourceKeyMember is null:
                        throw new NotSupportedException($"FK navigation '{nav.NavigationMember.Name}' on '{clrType.FullName}' must specify a foreign key selector (WithForeignKey) or be provided by schema conventions.");
                    case Db2ReferenceNavigationKind.ForeignKeyToPrimaryKey when nav.TargetKeyMember is null:
                        nav.TargetKeyMember = ResolvePrimaryKeyMember(Entity(nav.TargetClrType));
                        break;
                }

                if (kind == Db2ReferenceNavigationKind.ForeignKeyToPrimaryKey && nav.TargetKeyMember is null)
                    throw new NotSupportedException($"FK navigation '{nav.NavigationMember.Name}' on '{clrType.FullName}' must resolve a target primary key.");

                var targetMetadata = Entity(nav.TargetClrType);
                var targetTableName = targetMetadata.TableName ?? nav.TargetClrType.Name;
                var targetSchema = schemaResolver(targetTableName);

                var sourceKeyFieldSchema = ResolveFieldSchema(schema, m, nav.SourceKeyMember!, $"source key member '{nav.SourceKeyMember!.Name}' in navigation '{clrType.FullName}.{nav.NavigationMember.Name}'");
                var targetKeyFieldSchema = ResolveFieldSchema(targetSchema, targetMetadata, nav.TargetKeyMember!, $"target key member '{nav.TargetKeyMember!.Name}' in navigation '{clrType.FullName}.{nav.NavigationMember.Name}'");

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

                switch (kind)
                {
                    case Db2CollectionNavigationKind.ForeignKeyArrayToPrimaryKey:
                        {
                            if (nav.SourceKeyCollectionMember is null)
                                throw new NotSupportedException($"Collection navigation '{nav.NavigationMember.Name}' on '{clrType.FullName}' must specify a source key collection member (WithForeignKeyArray). ");

                            var sourceKeyCollectionType = nav.SourceKeyCollectionMember.GetMemberType();
                            if (!IsIntEnumerableType(sourceKeyCollectionType))
                            {
                                throw new NotSupportedException(
                                    $"Collection navigation '{clrType.FullName}.{nav.NavigationMember.Name}' expects an int key collection (e.g., int[] or ICollection<int>) but found '{sourceKeyCollectionType.FullName}'.");
                            }

                            var sourceKeyFieldSchema = ResolveFieldSchema(schema, m, nav.SourceKeyCollectionMember, $"source key member '{nav.SourceKeyCollectionMember.Name}' in collection navigation '{clrType.FullName}.{nav.NavigationMember.Name}'");

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

                    case Db2CollectionNavigationKind.DependentForeignKeyToPrimaryKey:
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

                            var dependentFkFieldSchema = ResolveFieldSchema(dependentSchema, dependentMetadata, nav.DependentForeignKeyMember, $"dependent FK member '{nav.DependentForeignKeyMember.Name}' in collection navigation '{clrType.FullName}.{nav.NavigationMember.Name}'");

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

                    default:
                        throw new NotSupportedException($"Collection navigation '{nav.NavigationMember.Name}' on '{clrType.FullName}' has unsupported kind '{kind}'.");
                }
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

    private static Db2FieldSchema ResolveFieldSchema(Db2TableSchema schema, Db2EntityTypeMetadata metadata, MemberInfo member, string context)
    {
        if (member is not PropertyInfo { GetMethod.IsPublic: true } property)
            throw new NotSupportedException($"Member '{metadata.ClrType.FullName}.{member.Name}' must be a public property to map to DB2 columns.");

        var memberName = ResolveColumnName(metadata, property);
        if (!schema.TryGetFieldCaseInsensitive(memberName, out var fieldSchema))
        {
            throw new NotSupportedException(
                $"Field '{memberName}' not found in schema for table '{schema.TableName}'. " +
                $"This field is required for {context}. " +
                $"Ensure the member name matches a field in the .dbd definition.");
        }

        return fieldSchema;
    }

    private static bool HasAnyColumnMapping(Db2EntityTypeMetadata metadata, MemberInfo member)
    {
        if (member is not PropertyInfo p)
            return false;

        return p.GetCustomAttribute<ColumnAttribute>(inherit: false) switch
        {
            not null => true,
            _ => metadata.ColumnNameMappings.ContainsKey(p.Name),
        };
    }

    private static string ResolveColumnName(Db2EntityTypeMetadata metadata, PropertyInfo property)
    {
        if (metadata.ColumnNameMappings.TryGetValue(property.Name, out var configured))
            return configured;

        var attr = property.GetCustomAttribute<ColumnAttribute>(inherit: false);
        return attr switch
        {
            not null when !string.IsNullOrWhiteSpace(attr.Name) => attr.Name,
            _ => property.Name,
        };
    }

    private static MemberInfo ResolvePrimaryKeyMember(Db2EntityTypeMetadata metadata)
    {
        if (metadata.PrimaryKeyMember is not null)
            return metadata.PrimaryKeyMember;

        var candidates = metadata.ClrType.GetMember("Id", BindingFlags.Instance | BindingFlags.Public);
        foreach (var m in candidates)
            if (m is PropertyInfo { GetMethod.IsPublic: true } p)
                return p;

        throw new NotSupportedException($"Entity type '{metadata.ClrType.FullName}' has no key member. Configure a primary key in OnModelCreating (e.g., modelBuilder.Entity<{metadata.ClrType.Name}>().HasKey(x => x.Id)).");
    }

    private static PropertyInfo? FindMember(Type type, string name)
    {
        var members = type.GetMember(name, BindingFlags.Instance | BindingFlags.Public);
        foreach (var m in members)
        {
            if (m is PropertyInfo { GetMethod.IsPublic: true } p)
                return p;
        }

        return null;
    }

    private static bool IsScalarForeignKeyProperty(PropertyInfo property)
    {
        return property.PropertyType.IsScalarType();
    }

    private static bool TryGetIEnumerableElementType(Type type, out Type? elementType)
    {
        if (type == typeof(string))
        {
            elementType = null;
            return false;
        }

        if (type.IsArray)
        {
            elementType = type.GetElementType();
            return elementType is not null;
        }

        if (type.IsGenericType && type.GetGenericTypeDefinition() == typeof(IEnumerable<>))
        {
            elementType = type.GetGenericArguments()[0];
            return true;
        }

        foreach (var i in type.GetInterfaces().Where(static i => i.IsGenericType && i.GetGenericTypeDefinition() == typeof(IEnumerable<>)))
        {
            elementType = i.GetGenericArguments()[0];
            return true;
        }

        elementType = null;
        return false;
    }

    private static string ParseSingleForeignKeyName(string name, Type clrType, PropertyInfo property)
    {
        var trimmed = name.Trim();
        if (trimmed.Contains(',', StringComparison.Ordinal))
        {
            throw new NotSupportedException(
                $"[ForeignKey] on '{clrType.FullName}.{property.Name}' does not support composite keys. Use fluent configuration instead.");
        }

        return trimmed;
    }

}
