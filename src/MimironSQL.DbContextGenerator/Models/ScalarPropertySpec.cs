namespace MimironSQL.DbContextGenerator.Models;

internal sealed class ScalarPropertySpec(string propertyName, string typeName, string initializer, string? columnName)
{
    /// <summary>
    /// Gets the C# property name.
    /// </summary>
    public string PropertyName { get; } = propertyName;

    /// <summary>
    /// Gets the CLR type name.
    /// </summary>
    public string TypeName { get; } = typeName;

    /// <summary>
    /// Gets the initializer source text for the generated property.
    /// </summary>
    public string Initializer { get; } = initializer;

    /// <summary>
    /// Gets the source column name when it differs from <see cref="PropertyName"/>.
    /// </summary>
    public string? ColumnName { get; } = columnName;
}
