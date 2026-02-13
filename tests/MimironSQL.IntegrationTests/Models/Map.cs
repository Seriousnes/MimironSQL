namespace MimironSQL;

public partial class MapEntity
{
    public virtual ICollection<MapChallengeModeEntity> MapChallengeModes { get; set; } = [];
}