namespace MimironSQL.Db2.Model;

public interface IDb2EntityTypeConfiguration<T>
{
    void Configure(Db2EntityTypeBuilder<T> builder);
}
