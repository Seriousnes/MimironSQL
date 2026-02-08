using System.ComponentModel.DataAnnotations.Schema;
using System.Reflection;

namespace MimironSQL.Db2.Model;

internal sealed partial class Db2EntityTypeBuilder<T>
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
        if (typeof(T).GetCustomAttribute<TableAttribute>(inherit: false) is not null)
            throw new NotSupportedException($"Entity type '{typeof(T).FullName}' has a [Table] attribute and cannot also be configured with ToTable().");

        Metadata.TableName = tableName;
        Metadata.TableNameWasConfigured = true;
        return this;
    }

    public Db2PropertyBuilder<T> Property<TProperty>(System.Linq.Expressions.Expression<System.Func<T, TProperty>> property)
    {
        var p = Db2PropertyBuilder<T>.ResolveProperty(property);
        return new Db2PropertyBuilder<T>(Metadata, p);
    }
}
