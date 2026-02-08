using MimironSQL.Db2.Model;
using MimironSQL.EntityFrameworkCore.Db2.Model;

namespace MimironSQL.EntityFrameworkCore.TestConfigs;

public sealed class DuplicateEntity
{
    public int Id { get; set; }
}

public sealed class DuplicateConfigA : IDb2EntityTypeConfiguration<DuplicateEntity>
{
    void IDb2EntityTypeConfiguration<DuplicateEntity>.Configure(Db2EntityTypeBuilder<DuplicateEntity> builder)
        => builder.HasKey(x => x.Id);
}

public sealed class DuplicateConfigB : IDb2EntityTypeConfiguration<DuplicateEntity>
{
    void IDb2EntityTypeConfiguration<DuplicateEntity>.Configure(Db2EntityTypeBuilder<DuplicateEntity> builder)
        => builder.HasKey(x => x.Id);
}
