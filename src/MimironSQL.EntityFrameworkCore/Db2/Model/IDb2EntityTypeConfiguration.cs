namespace MimironSQL.Db2.Model;

internal interface IDb2EntityTypeConfiguration<T>
{
    void Configure(Db2EntityTypeBuilder<T> builder);
}
