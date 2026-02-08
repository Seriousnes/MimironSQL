using MimironSQL.Db2.Model;

namespace MimironSQL.EntityFrameworkCore.Db2.Model;

internal interface IDb2EntityTypeConfiguration<T>
{
    void Configure(Db2EntityTypeBuilder<T> builder);
}
