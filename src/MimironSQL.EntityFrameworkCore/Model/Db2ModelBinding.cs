using System.Collections.Concurrent;
using System.ComponentModel.DataAnnotations.Schema;
using System.Reflection;

using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;

using MimironSQL.EntityFrameworkCore.Extensions;
using MimironSQL.EntityFrameworkCore.Schema;

namespace MimironSQL.EntityFrameworkCore.Model;

internal sealed class Db2ModelBinding(IModel efModel, Func<string, Db2TableSchema> schemaResolver)
{
    private readonly IModel _efModel = efModel ?? throw new ArgumentNullException(nameof(efModel));
    private readonly Func<string, Db2TableSchema> _schemaResolver = schemaResolver ?? throw new ArgumentNullException(nameof(schemaResolver));

    private readonly ConcurrentDictionary<Type, Lazy<Db2EntityType>> _entityTypes = new();
    private readonly ConcurrentDictionary<(Type SourceClrType, MemberInfo NavigationMember), Lazy<Db2ReferenceNavigation?>> _referenceNavigations = new();
    private readonly ConcurrentDictionary<(Type SourceClrType, MemberInfo NavigationMember), Lazy<Db2CollectionNavigation>> _collectionNavigations = new();
    private readonly ConcurrentDictionary<Type, IReadOnlyList<MemberInfo>> _autoIncludeNavigations = new();

    internal IModel EfModel => _efModel;

    public Db2EntityType GetEntityType(Type clrType)
    {
        ArgumentNullException.ThrowIfNull(clrType);

        var lazy = _entityTypes.GetOrAdd(
            clrType,
            static (t, self) => new Lazy<Db2EntityType>(() => self.BuildEntityType(t), LazyThreadSafetyMode.ExecutionAndPublication),
            this);

        return lazy.Value;
    }

    public bool TryGetReferenceNavigation(Type sourceClrType, MemberInfo navigationMember, out Db2ReferenceNavigation? navigation)
    {
        ArgumentNullException.ThrowIfNull(sourceClrType);
        ArgumentNullException.ThrowIfNull(navigationMember);

        var canonicalMember = CanonicalizeMember(sourceClrType, navigationMember);
        var key = (sourceClrType, canonicalMember);

        var lazy = _referenceNavigations.GetOrAdd(
            key,
            static (k, self) => new Lazy<Db2ReferenceNavigation?>(() => self.BuildReferenceNavigation(k.SourceClrType, k.NavigationMember), LazyThreadSafetyMode.ExecutionAndPublication),
            this);

        try
        {
            navigation = lazy.Value;
            return navigation is not null;
        }
        catch (KeyNotFoundException)
        {
            navigation = null;
            return false;
        }
    }

    public bool TryGetCollectionNavigation(Type sourceClrType, MemberInfo navigationMember, out Db2CollectionNavigation navigation)
    {
        ArgumentNullException.ThrowIfNull(sourceClrType);
        ArgumentNullException.ThrowIfNull(navigationMember);

        var canonicalMember = CanonicalizeMember(sourceClrType, navigationMember);
        var key = (sourceClrType, canonicalMember);

        var lazy = _collectionNavigations.GetOrAdd(
            key,
            static (k, self) => new Lazy<Db2CollectionNavigation>(() => self.BuildCollectionNavigation(k.SourceClrType, k.NavigationMember), LazyThreadSafetyMode.ExecutionAndPublication),
            this);

        try
        {
            navigation = lazy.Value;
            return true;
        }
        catch (KeyNotFoundException)
        {
            navigation = null!;
            return false;
        }
    }

    public IReadOnlyList<MemberInfo> GetAutoIncludeNavigations(Type sourceClrType)
    {
        ArgumentNullException.ThrowIfNull(sourceClrType);

        return _autoIncludeNavigations.GetOrAdd(sourceClrType, static (t, self) => self.BuildAutoIncludeNavigations(t), this);
    }

    private Db2EntityType BuildEntityType(Type clrType)
    {
        ValidateEntityType(clrType);
        ValidateTypeAttributes(clrType);

        var efEntityType = _efModel.FindEntityType(clrType)
            ?? throw new KeyNotFoundException($"No entity type registered for CLR type '{clrType.FullName}'.");

        var tableName = efEntityType.GetTableName() ?? clrType.Name;

        try
        {
            var schema = _schemaResolver(tableName);

            if (efEntityType.FindPrimaryKey() is not { Properties.Count: 1 } pk)
            {
                throw new NotSupportedException(
                    $"Entity type '{clrType.FullName}' has no key member. Configure a primary key in OnModelCreating (e.g., modelBuilder.Entity<{clrType.Name}>().HasKey(x => x.Id)).");
            }

            var pkProperty = pk.Properties[0];
            if (pkProperty.PropertyInfo is not { GetMethod.IsPublic: true } pkMember)
            {
                throw new NotSupportedException(
                    $"Entity type '{clrType.FullName}' primary key must be a public property. Configure a key in OnModelCreating (e.g., modelBuilder.Entity<{clrType.Name}>().HasKey(x => x.Id)).");
            }

            var columnNameMappings = new Dictionary<string, string>(StringComparer.Ordinal);

            foreach (var property in efEntityType.GetProperties())
            {
                if (property.PropertyInfo is not { } propInfo)
                {
                    continue;
                }

                if (property.IsShadowProperty())
                {
                    continue;
                }

                if (HasColumnAttribute(propInfo))
                {
                    continue;
                }

                var columnName = property.GetColumnName() ?? property.Name;
                if (string.Equals(columnName, propInfo.Name, StringComparison.Ordinal))
                {
                    continue;
                }

                columnNameMappings[propInfo.Name] = columnName;
            }

            var pkFieldSchema = ResolveFieldSchema(schema, columnNameMappings, pkMember, $"primary key member '{pkMember.Name}' of entity '{clrType.FullName}'");

            return new Db2EntityType(
                clrType: clrType,
                tableName: tableName,
                schema: schema,
                primaryKeyMember: pkMember,
                primaryKeyFieldSchema: pkFieldSchema,
                columnNameMappings: columnNameMappings);
        }
        catch (FileNotFoundException ex)
        {
            throw new FileNotFoundException(
                $"DB2 file not found for table '{tableName}' (CLR type '{clrType.FullName}').",
                ex);
        }
    }

    private static void ValidateEntityType(Type clrType)
    {
        if (!(clrType.BaseType is { IsGenericType: true } baseType && baseType.GetGenericTypeDefinition() == typeof(Db2Entity<>)))
        {
            throw new NotSupportedException($"Entity type '{clrType.FullName}' must derive from '{typeof(Db2Entity<>).FullName}' to be queried/materialized by MimironSQL.");
        }
    }

    private Db2ReferenceNavigation? BuildReferenceNavigation(Type sourceClrType, MemberInfo navigationMember)
    {
        var sourceEntityEf = _efModel.FindEntityType(sourceClrType);
        if (sourceEntityEf is null)
        {
            return null;
        }

        var nav = sourceEntityEf.FindNavigation(navigationMember.Name);
        if (nav is null || nav.PropertyInfo is null)
        {
            return null;
        }

        if (nav.IsCollection)
        {
            return null;
        }

        var fk = nav.ForeignKey;

        if (fk.Properties.Count != 1 || fk.PrincipalKey.Properties.Count != 1)
        {
            return null;
        }

        var sourceClr = sourceClrType;
        var targetClr = nav.TargetEntityType.ClrType;

        try
        {
            // ForeignKeyToPrimaryKey: FK is on the entity declaring the navigation.
            if (fk.DeclaringEntityType.ClrType == sourceClr)
            {
                if (fk.Properties[0].PropertyInfo is not { } sourceFkProp)
                {
                    return null;
                }

                if (fk.PrincipalKey.Properties[0].PropertyInfo is not { } principalKey)
                {
                    return null;
                }

                var sourceEntity = GetEntityType(sourceClr);
                var targetEntity = GetEntityType(targetClr);

                return new Db2ReferenceNavigation(
                    sourceClrType: sourceClr,
                    navigationMember: CanonicalizeMember(sourceClr, nav.PropertyInfo),
                    targetClrType: targetClr,
                    kind: Db2ReferenceNavigationKind.ForeignKeyToPrimaryKey,
                    sourceKeyMember: sourceFkProp,
                    targetKeyMember: principalKey,
                    sourceKeyFieldSchema: sourceEntity.ResolveFieldSchema(sourceFkProp, $"resolving navigation '{sourceClr.FullName}.{navigationMember.Name}'"),
                    targetKeyFieldSchema: targetEntity.ResolveFieldSchema(principalKey, $"resolving navigation '{sourceClr.FullName}.{navigationMember.Name}'"));
            }

            // SharedPrimaryKeyOneToOne: navigation is on the principal, but the FK is the dependent PK.
            if (fk.IsUnique
                && fk.DeclaringEntityType.ClrType == targetClr
                && fk.DeclaringEntityType.FindPrimaryKey() is { Properties.Count: 1 } dependentPk
                && ReferenceEquals(dependentPk.Properties[0], fk.Properties[0]))
            {
                var principalKey = fk.PrincipalKey.Properties[0].PropertyInfo;
                var dependentKey = dependentPk.Properties[0].PropertyInfo;

                if (principalKey is null || dependentKey is null)
                {
                    throw new KeyNotFoundException();
                }

                if (principalKey.PropertyType != dependentKey.PropertyType)
                {
                    throw new KeyNotFoundException();
                }

                var sourceEntity = GetEntityType(sourceClr);
                var targetEntity = GetEntityType(targetClr);

                return new Db2ReferenceNavigation(
                    sourceClrType: sourceClr,
                    navigationMember: CanonicalizeMember(sourceClr, nav.PropertyInfo),
                    targetClrType: targetClr,
                    kind: Db2ReferenceNavigationKind.SharedPrimaryKeyOneToOne,
                    sourceKeyMember: principalKey,
                    targetKeyMember: dependentKey,
                    sourceKeyFieldSchema: sourceEntity.ResolveFieldSchema(principalKey, $"resolving navigation '{sourceClr.FullName}.{navigationMember.Name}'"),
                    targetKeyFieldSchema: targetEntity.ResolveFieldSchema(dependentKey, $"resolving navigation '{sourceClr.FullName}.{navigationMember.Name}'"));
            }

            throw new KeyNotFoundException();
        }
        catch (FileNotFoundException ex)
        {
            throw new FileNotFoundException(
                $"DB2 file not found while resolving navigation '{sourceClrType.FullName}.{navigationMember.Name}'.",
                ex);
        }
    }

    private Db2CollectionNavigation BuildCollectionNavigation(Type sourceClrType, MemberInfo navigationMember)
    {
        var sourceEntityEf = _efModel.FindEntityType(sourceClrType) ?? throw new KeyNotFoundException();
        var nav = sourceEntityEf.FindNavigation(navigationMember.Name);
        if (nav is null || nav.PropertyInfo is null)
        {
            throw new KeyNotFoundException();
        }

        if (!nav.IsCollection)
        {
            throw new KeyNotFoundException();
        }

        var targetClr = nav.TargetEntityType.ClrType;

        ValidateCollectionNavigationMemberType(sourceClrType, nav.PropertyInfo, targetClr);

        try
        {
            if (Db2ForeignKeyArrayAnnotations.TryGetForeignKeyArrayPropertyName(nav, out var foreignKeyArrayPropertyName))
            {
                var sourceKeyCollectionMember = FindMember(sourceClrType, foreignKeyArrayPropertyName)
                    ?? throw new NotSupportedException(
                        $"FK array property '{sourceClrType.FullName}.{foreignKeyArrayPropertyName}' was not found. Ensure it is a public property.");

                var sourceKeyCollectionType = sourceKeyCollectionMember.GetMemberType();
                if (!IsIntKeyEnumerableType(sourceKeyCollectionType))
                {
                    throw new NotSupportedException(
                        $"Collection navigation '{sourceClrType.FullName}.{nav.PropertyInfo.Name}' expects an integer key collection (e.g., int[] or ICollection<int>) but found '{sourceKeyCollectionType.FullName}'.");
                }

                var sourceEntity = GetEntityType(sourceClrType);
                var sourceKeyFieldSchema = sourceEntity.ResolveFieldSchema(sourceKeyCollectionMember, $"source key member '{sourceKeyCollectionMember.Name}' in collection navigation '{sourceClrType.FullName}.{nav.PropertyInfo.Name}'");

                return new Db2CollectionNavigation(
                    sourceClrType: sourceClrType,
                    navigationMember: CanonicalizeMember(sourceClrType, nav.PropertyInfo),
                    targetClrType: targetClr,
                    kind: Db2CollectionNavigationKind.ForeignKeyArrayToPrimaryKey,
                    sourceKeyCollectionMember: sourceKeyCollectionMember,
                    sourceKeyFieldSchema: sourceKeyFieldSchema,
                    dependentForeignKeyMember: null,
                    dependentForeignKeyFieldSchema: null,
                    principalKeyMember: null);
            }

            var fk = nav.ForeignKey;

            if (fk.Properties.Count != 1 || fk.PrincipalKey.Properties.Count != 1)
            {
                throw new KeyNotFoundException();
            }

            if (fk.Properties[0].PropertyInfo is not { } dependentFkMember)
            {
                throw new KeyNotFoundException();
            }

            var principalKeyMember = fk.PrincipalKey.Properties[0].PropertyInfo ?? throw new KeyNotFoundException();
            var principalKeyType = principalKeyMember.GetMemberType();
            if (!principalKeyType.IsScalarType())
            {
                throw new NotSupportedException($"Principal key member '{sourceClrType.FullName}.{principalKeyMember.Name}' must be a scalar type.");
            }

            var dependentEntity = GetEntityType(targetClr);
            var dependentFkFieldSchema = dependentEntity.ResolveFieldSchema(dependentFkMember, $"dependent FK member '{dependentFkMember.Name}' in collection navigation '{sourceClrType.FullName}.{nav.PropertyInfo.Name}'");

            return new Db2CollectionNavigation(
                sourceClrType: sourceClrType,
                navigationMember: CanonicalizeMember(sourceClrType, nav.PropertyInfo),
                targetClrType: targetClr,
                kind: Db2CollectionNavigationKind.DependentForeignKeyToPrimaryKey,
                sourceKeyCollectionMember: null,
                sourceKeyFieldSchema: null,
                dependentForeignKeyMember: dependentFkMember,
                dependentForeignKeyFieldSchema: dependentFkFieldSchema,
                principalKeyMember: principalKeyMember);
        }
        catch (FileNotFoundException ex)
        {
            throw new FileNotFoundException(
                $"DB2 file not found while resolving collection navigation '{sourceClrType.FullName}.{navigationMember.Name}'.",
                ex);
        }
    }

    private IReadOnlyList<MemberInfo> BuildAutoIncludeNavigations(Type sourceClrType)
    {
        var entityType = _efModel.FindEntityType(sourceClrType);
        if (entityType is null)
        {
            return [];
        }

        return [
            .. entityType.GetNavigations()
                .Where(static n => n.IsEagerLoaded && n.PropertyInfo is not null)
                .Select(static n => n.PropertyInfo!)
                .OrderBy(static m => m.Name, StringComparer.Ordinal),
        ];
    }

    private static Db2FieldSchema ResolveFieldSchema(
        Db2TableSchema schema,
        IReadOnlyDictionary<string, string> columnNameMappings,
        PropertyInfo property,
        string context)
    {
        var memberName = ResolveColumnName(columnNameMappings, property);
        if (!schema.TryGetFieldCaseInsensitive(memberName, out var fieldSchema))
        {
            throw new NotSupportedException(
                $"Field '{memberName}' not found in schema for table '{schema.TableName}'. " +
                $"This field is required for {context}. " +
                "Ensure the member name matches a field in the .dbd definition.");
        }

        return fieldSchema;
    }

    private static string ResolveColumnName(IReadOnlyDictionary<string, string> columnNameMappings, PropertyInfo property)
    {
        if (columnNameMappings.TryGetValue(property.Name, out var configured))
        {
            return configured;
        }

        var attr = property.GetCustomAttribute<ColumnAttribute>(inherit: false);
        return attr switch
        {
            not null when !string.IsNullOrWhiteSpace(attr.Name) => attr.Name,
            _ => property.Name,
        };
    }

    private static void ValidateTypeAttributes(Type clrType)
    {
        foreach (var f in clrType.GetFields(BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic))
        {
            if (f.GetCustomAttribute<ColumnAttribute>(inherit: false) is not null)
            {
                throw new NotSupportedException($"Column mapping attributes are only supported on public properties. Field '{clrType.FullName}.{f.Name}' is not a valid target.");
            }

            if (f.GetCustomAttribute<ForeignKeyAttribute>(inherit: false) is not null)
            {
                throw new NotSupportedException($"Foreign key mapping attributes are only supported on public properties. Field '{clrType.FullName}.{f.Name}' is not a valid target.");
            }
        }

        foreach (var p in clrType.GetProperties(BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic))
        {
            if (p.GetCustomAttribute<ColumnAttribute>(inherit: false) is null)
            {
                if (p.GetCustomAttribute<ForeignKeyAttribute>(inherit: false) is null)
                {
                    continue;
                }

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
    }

    private static bool HasColumnAttribute(PropertyInfo property)
        => property.GetCustomAttribute<ColumnAttribute>(inherit: false) is not null;

    private static void ValidateCollectionNavigationMemberType(Type sourceClrType, MemberInfo navigationMember, Type targetClrType)
    {
        if (navigationMember is not PropertyInfo p)
        {
            throw new NotSupportedException($"Collection navigation '{sourceClrType.FullName}.{navigationMember.Name}' must be a property.");
        }

        if (p.GetMethod is not { IsPublic: true })
        {
            throw new NotSupportedException($"Collection navigation '{sourceClrType.FullName}.{p.Name}' must have a public getter.");
        }

        if (p.SetMethod is not { IsPublic: true })
        {
            throw new NotSupportedException($"Collection navigation '{sourceClrType.FullName}.{p.Name}' must have a public setter.");
        }

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
        {
            return false;
        }

        return IsIntKeyType(elementType);
    }

    private static bool IsIntKeyType(Type type)
    {
        var underlying = type.UnwrapNullable();
        if (underlying.IsEnum)
        {
            underlying = Enum.GetUnderlyingType(underlying);
        }

        return underlying == typeof(byte)
            || underlying == typeof(sbyte)
            || underlying == typeof(short)
            || underlying == typeof(ushort)
            || underlying == typeof(int)
            || underlying == typeof(uint)
            || underlying == typeof(long)
            || underlying == typeof(ulong);
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

    private static PropertyInfo? FindMember(Type type, string name)
    {
        var members = type.GetMember(name, BindingFlags.Instance | BindingFlags.Public);
        foreach (var m in members)
        {
            if (m is PropertyInfo { GetMethod.IsPublic: true } p)
            {
                return p;
            }
        }

        return null;
    }

    private static MemberInfo CanonicalizeMember(Type sourceClrType, MemberInfo member)
    {
        if (member is PropertyInfo p)
        {
            return sourceClrType.GetProperty(p.Name, BindingFlags.Instance | BindingFlags.Public) ?? member;
        }

        return member;
    }
}