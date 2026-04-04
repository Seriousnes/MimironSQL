using System.Collections.Immutable;

using Microsoft.CodeAnalysis;

using MimironSQL.Dbd;
using MimironSQL.Db2;
using MimironSQL.DbContextGenerator.Utility;

namespace MimironSQL.DbContextGenerator.Models;

internal sealed class EntitySpec(
        string tableName,
        string className,
        string idColumnName,
        string idTypeName,
        ImmutableArray<ScalarPropertySpec> scalarProperties,
        ImmutableArray<NavigationSpec> navigations)
{
    /// <summary>
    /// Gets the source table name.
    /// </summary>
    public string TableName { get; } = tableName;

    /// <summary>
    /// Gets the generated CLR type name.
    /// </summary>
    public string ClassName { get; } = className;

    /// <summary>
    /// Gets the DB2 schema column name that backs the <c>Id</c> property.
    /// </summary>
    public string IdColumnName { get; } = idColumnName;

    /// <summary>
    /// Gets the CLR type name used for the entity key.
    /// </summary>
    public string IdTypeName { get; } = idTypeName;

    /// <summary>
    /// Gets the scalar property specifications for the entity.
    /// </summary>
    public ImmutableArray<ScalarPropertySpec> ScalarProperties { get; } = scalarProperties;

    /// <summary>
    /// Gets the navigation property specifications for the entity.
    /// </summary>
    public ImmutableArray<NavigationSpec> Navigations { get; } = navigations;

    /// <summary>
    /// Creates an entity specification from a DBD file and a selected build block.
    /// </summary>
    /// <param name="tableName">The source table name.</param>
    /// <param name="dbd">The parsed DBD file.</param>
    /// <param name="build">The selected build block.</param>
    /// <returns>The created entity specification.</returns>
    public static EntitySpec Create(string tableName, DbdFile dbd, DbdBuildBlock build)
    {
        var className = $"{NameNormalizer.NormalizeTypeName(tableName)}Entity";

        var idEntry = build.Entries.FirstOrDefault(e => e.IsId);
        var keyEntry = idEntry ?? build.Entries.FirstOrDefault(static e =>
            e is { ElementCount: 1 } && (e.ValueType == Db2ValueType.Int64 || e.ValueType == Db2ValueType.UInt64 || TypeMapping.TryMapInlineInteger(e.InlineTypeToken, out _)));

        var idColumnName = keyEntry?.Name ?? "ID";
        var idType = TypeMapping.GetIdClrType(keyEntry, dbd.ColumnsByName);

        var scalarProperties = new List<ScalarPropertySpec>();
        var navigations = new List<NavigationSpec>();
        var usedNames = new HashSet<string>(StringComparer.Ordinal);
        var scalarPropertyNameByColumnName = new Dictionary<string, (string EscapedPropertyName, string UnescapedPropertyName)>(StringComparer.Ordinal);

        foreach (var entry in build.Entries.Where(e => !ReferenceEquals(e, keyEntry) && !e.IsId
            && !(e.ElementCount > 1 && (e.ValueType == Db2ValueType.String || e.ValueType == Db2ValueType.LocString))))
        {
            var columnName = entry.Name;
            var propertyName = NameNormalizer.NormalizePropertyName(columnName);
            propertyName = NameNormalizer.MakeUnique(propertyName, usedNames);

            var typeName = TypeMapping.GetClrTypeName(entry);
            var initializer = TypeMapping.GetInitializer(typeName);

            string? mappedColumnName = null;
            if (!string.Equals(propertyName, columnName, StringComparison.Ordinal))
            {
                mappedColumnName = columnName;
            }

            var escapedPropertyName = NameNormalizer.EscapeIdentifier(propertyName);

            scalarProperties.Add(new ScalarPropertySpec(
                escapedPropertyName,
                typeName,
                initializer,
                mappedColumnName));

            scalarPropertyNameByColumnName[columnName] = (escapedPropertyName, propertyName);
        }

        foreach (var entry in build.Entries.Where(e => !ReferenceEquals(e, keyEntry) && !e.IsId))
        {
            if (entry.ReferencedTableName is not { Length: > 0 } targetTable)
            {
                continue;
            }

            var columnName = entry.Name;
            if (!scalarPropertyNameByColumnName.TryGetValue(columnName, out var scalarProperty))
            {
                continue;
            }

            var rawNavName = columnName.EndsWith("ID", StringComparison.Ordinal)
                ? columnName.Substring(0, columnName.Length - 2)
                : columnName;

            var navName = NameNormalizer.NormalizePropertyName(rawNavName);
            if (string.Equals(navName, scalarProperty.UnescapedPropertyName, StringComparison.Ordinal))
            {
                navName += entry.ElementCount > 1 ? "Collection" : "Entity";
            }

            navName = NameNormalizer.MakeUnique(navName, usedNames);

            var isForeignKeyArray = entry.ElementCount > 1;

            navigations.Add(new NavigationSpec(
                targetTableName: targetTable,
                foreignKeyPropertyName: scalarProperty.EscapedPropertyName,
                propertyName: NameNormalizer.EscapeIdentifier(navName),
                isCollection: isForeignKeyArray,
                isForeignKeyArray: isForeignKeyArray));
        }

        return new EntitySpec(tableName, className, idColumnName, idType, [.. scalarProperties], [.. navigations]);
    }
}