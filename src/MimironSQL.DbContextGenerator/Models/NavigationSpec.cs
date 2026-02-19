namespace MimironSQL.DbContextGenerator.Models;

internal sealed class NavigationSpec(string targetTableName, string foreignKeyPropertyName, string propertyName, bool isCollection)
{
    /// <summary>
    /// Gets the target table name for the navigation.
    /// </summary>
    public string TargetTableName { get; } = targetTableName;

    /// <summary>
    /// Gets the foreign key property name in the source entity.
    /// </summary>
    public string ForeignKeyPropertyName { get; } = foreignKeyPropertyName;

    /// <summary>
    /// Gets the navigation property name.
    /// </summary>
    public string PropertyName { get; } = propertyName;

    /// <summary>
    /// Gets a value indicating whether the navigation is a collection.
    /// </summary>
    public bool IsCollection { get; } = isCollection;

    /// <summary>
    /// Gets a value indicating whether the navigation is configured using <c>HasForeignKeyArray</c>.
    /// </summary>
    public bool IsForeignKeyArray { get; } = false;

    public NavigationSpec(string targetTableName, string foreignKeyPropertyName, string propertyName, bool isCollection, bool isForeignKeyArray)
        : this(targetTableName, foreignKeyPropertyName, propertyName, isCollection)
    {
        IsForeignKeyArray = isForeignKeyArray;
    }
}
