using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Builders;

namespace MimironSQL;

public class TactKeyEntityConfiguration : IEntityTypeConfiguration<TactKeyEntity>
{
    public void Configure(EntityTypeBuilder<TactKeyEntity> builder)
    {
        builder.ToTable("TactKey");
        builder.HasKey(x => x.Id);
    }
}
