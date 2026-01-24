namespace MimironSQL.Db2.Model;

public sealed partial class Db2EntityTypeBuilder<T>
{
    private readonly Db2ModelBuilder _modelBuilder;

    internal Db2EntityTypeBuilder(Db2ModelBuilder modelBuilder, Db2EntityTypeMetadata metadata)
    {
        _modelBuilder = modelBuilder;
        Metadata = metadata;
    }

    internal Db2EntityTypeMetadata Metadata { get; }

    public Db2EntityTypeBuilder<T> ToTable(string tableName)
    {
        Metadata.TableName = tableName;
        return this;
    }
}
