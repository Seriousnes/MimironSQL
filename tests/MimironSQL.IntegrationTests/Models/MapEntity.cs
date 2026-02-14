using Microsoft.EntityFrameworkCore.Metadata.Builders;

namespace MimironSQL;

public partial class MapEntity
{
    public virtual ICollection<MapChallengeModeEntity> MapChallengeModes { get; set; } = [];
}

public partial class MapEntityConfiguration
{
    partial void ConfigureNavigation(EntityTypeBuilder<MapEntity> builder)
    {
        builder.HasMany(x => x.MapChallengeModes)
            .WithOne(x => x.Map)
            .HasForeignKey(x => x.MapID);
    }
}
