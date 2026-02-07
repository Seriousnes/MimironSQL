using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;

namespace MimironSQL.EntityFrameworkCore;

public class MimironDb2DbContextOptionsBuilder
{
    public MimironDb2DbContextOptionsBuilder(DbContextOptionsBuilder optionsBuilder)
    {
        ArgumentNullException.ThrowIfNull(optionsBuilder);
        OptionsBuilder = optionsBuilder;
    }

    protected DbContextOptionsBuilder OptionsBuilder { get; }
}
