using Microsoft.EntityFrameworkCore;

namespace MimironSQL.EntityFrameworkCore.Infrastructure;

public sealed class MimironDb2OptionsBuilder(DbContextOptionsBuilder optionsBuilder)
{
    public DbContextOptionsBuilder OptionsBuilder { get; } = optionsBuilder ?? throw new ArgumentNullException(nameof(optionsBuilder));
}
