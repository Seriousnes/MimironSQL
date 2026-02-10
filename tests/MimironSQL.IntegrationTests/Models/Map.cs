namespace MimironSQL;

public partial class MapEntity
{
    public ICollection<MapChallengeModeEntity> MapChallengeModes { get; set; } = [];
}