using System.Reflection;

namespace MimironSQL.Db2.Model;

internal sealed class Db2EntityTypeMetadata(Type clrType)
{
    public Type ClrType { get; } = clrType;

    public string? TableName { get; set; }

    public bool TableNameWasConfigured { get; set; }

    public MemberInfo? PrimaryKeyMember { get; set; }

    public bool PrimaryKeyWasConfigured { get; set; }

    public Dictionary<string, string> ColumnNameMappings { get; } = new(StringComparer.Ordinal);

    public List<Db2NavigationMetadata> Navigations { get; } = [];

    public List<Db2CollectionNavigationMetadata> CollectionNavigations { get; } = [];
}
