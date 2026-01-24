namespace MimironSQL.Db2.Model;

internal sealed class Db2EntityTypeMetadata(Type clrType)
{
    public Type ClrType { get; } = clrType;

    public string? TableName { get; set; }

    public List<Db2NavigationMetadata> Navigations { get; } = [];
}
