using System.Reflection;
using System.ComponentModel.DataAnnotations.Schema;

using MimironSQL.EntityFrameworkCore.Db2.Schema;
using MimironSQL.EntityFrameworkCore.Extensions;
using MimironSQL.Db2.Model;

namespace MimironSQL.EntityFrameworkCore.Db2.Model;

internal sealed class Db2ModelBuilder
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

    internal void SetAutoInclude(Type sourceClrType, MemberInfo navigationMember)
    {
        ArgumentNullException.ThrowIfNull(sourceClrType);
        ArgumentNullException.ThrowIfNull(navigationMember);

        var metadata = Entity(sourceClrType);
        metadata.AutoIncludeNavigations.Add(navigationMember);
    }

    internal Db2Model Build(Func<string, Db2TableSchema> schemaResolver)
    {
        ArgumentNullException.ThrowIfNull(schemaResolver);

        var built = new Dictionary<Type, Db2EntityType>(_entityTypes.Count);

        var navigations = new Dictionary<(Type SourceClrType, MemberInfo NavigationMember), Db2ReferenceNavigation>();
        var collectionNavigations = new Dictionary<(Type SourceClrType, MemberInfo NavigationMember), Db2CollectionNavigation>();
        var autoIncludes = new Dictionary<Type, IReadOnlyList<MemberInfo>>(_entityTypes.Count);

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

            if (m.AutoIncludeNavigations.Count != 0)
                autoIncludes[clrType] = [.. m.AutoIncludeNavigations.OrderBy(static m => m.Name, StringComparer.Ordinal)];

            foreach (var nav in m.Navigations)
            {
                if (nav.Kind is not { } kind)
                    throw new NotSupportedException($"Navigation '{nav.NavigationMember.Name}' on '{clrType.FullName}' must be configured (e.g., WithSharedPrimaryKey).");

                switch (kind)
                {
                    case Db2ReferenceNavigationKind.SharedPrimaryKeyOneToOne when (nav.SourceKeyMember is null || nav.TargetKeyMember is null):
                        throw new NotSupportedException($"Shared primary key navigation '{nav.NavigationMember.Name}' on '{clrType.FullName}' must specify both key selectors.");
                    case Db2ReferenceNavigationKind.ForeignKeyToPrimaryKey when nav.SourceKeyMember is null:
                        throw new NotSupportedException($"FK navigation '{nav.NavigationMember.Name}' on '{clrType.FullName}' must specify a foreign key selector (WithForeignKey).");
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
                    targetKeyFieldSchema: targetKeyFieldSchema);
            }

            foreach (var nav in m.CollectionNavigations)
            {
                if (nav.Kind is not { } kind)
                    throw new NotSupportedException($"Collection navigation '{nav.NavigationMember.Name}' on '{clrType.FullName}' must be configured (e.g., WithForeignKeyArray). ");

                ValidateCollectionNavigationMemberType(clrType, nav.NavigationMember, nav.TargetClrType);

                switch (kind)
                {
                    case Db2CollectionNavigationKind.ForeignKeyArrayToPrimaryKey:
                        {
                            if (nav.SourceKeyCollectionMember is null)
                                throw new NotSupportedException($"Collection navigation '{nav.NavigationMember.Name}' on '{clrType.FullName}' must specify a source key collection member (WithForeignKeyArray). ");

                            var sourceKeyCollectionType = nav.SourceKeyCollectionMember.GetMemberType();
                            if (!IsIntKeyEnumerableType(sourceKeyCollectionType))
                            {
                                throw new NotSupportedException(
                                    $"Collection navigation '{clrType.FullName}.{nav.NavigationMember.Name}' expects an integer key collection (e.g., int[] or ICollection<int>) but found '{sourceKeyCollectionType.FullName}'.");
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
                                principalKeyMember: null);

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
                                principalKeyMember: principalKeyMember);

                            continue;
                        }

                    default:
                        throw new NotSupportedException($"Collection navigation '{nav.NavigationMember.Name}' on '{clrType.FullName}' has unsupported kind '{kind}'.");
                }
            }
        }

        return new Db2Model(built, navigations, collectionNavigations, autoIncludes);
    }

    private static void ValidateCollectionNavigationMemberType(Type sourceClrType, MemberInfo navigationMember, Type targetClrType)
    {
        if (navigationMember is not PropertyInfo p)
            throw new NotSupportedException($"Collection navigation '{sourceClrType.FullName}.{navigationMember.Name}' must be a property.");

        if (p.GetMethod is not { IsPublic: true })
            throw new NotSupportedException($"Collection navigation '{sourceClrType.FullName}.{p.Name}' must have a public getter.");

        if (p.SetMethod is not { IsPublic: true })
            throw new NotSupportedException($"Collection navigation '{sourceClrType.FullName}.{p.Name}' must have a public setter.");

        var expectedType = typeof(ICollection<>).MakeGenericType(targetClrType);
        if (p.PropertyType != expectedType)
        {
            throw new NotSupportedException(
                $"Collection navigation '{sourceClrType.FullName}.{p.Name}' must be declared as '{expectedType.FullName}' but found '{p.PropertyType.FullName}'.");
        }
    }

    private static bool IsIntKeyEnumerableType(Type type)
    {
        if (!TryGetIEnumerableElementType(type, out var elementType) || elementType is null)
            return false;

        return IsIntKeyType(elementType);
    }

    private static bool IsIntKeyType(Type type)
    {
        var underlying = type.UnwrapNullable();
        if (underlying.IsEnum)
            underlying = Enum.GetUnderlyingType(underlying);

        return underlying == typeof(byte)
            || underlying == typeof(sbyte)
            || underlying == typeof(short)
            || underlying == typeof(ushort)
            || underlying == typeof(int)
            || underlying == typeof(uint)
            || underlying == typeof(long)
            || underlying == typeof(ulong);
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
}
