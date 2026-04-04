using Microsoft.EntityFrameworkCore.Metadata.Builders;

using MimironSQL.EntityFrameworkCore.Model;

namespace MimironSQL;

public partial class SpellEntity
{
    public virtual SpellNameEntity SpellName { get; set; } = null!;
}

public partial class SpellNameEntity
{    
    public virtual SpellEntity Spell { get; set; } = null!;
}

public partial class SpellEntityConfiguration
{
    partial void ConfigureNavigation(EntityTypeBuilder<SpellEntity> builder)
    {
        builder.HasSharedPrimaryKey(
            spell => spell.SpellName,
            spellName => spellName.Spell);
    }
}
