using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.EntityFrameworkCore.Update;

namespace MimironSQL.EntityFrameworkCore.Storage.Internal;

internal sealed class MimironDb2Database(DatabaseDependencies dependencies) : Database(dependencies)
{
    public override int SaveChanges(IList<IUpdateEntry> entries)
        => throw new NotSupportedException("MimironDb2 is a read-only provider.");

    public override Task<int> SaveChangesAsync(IList<IUpdateEntry> entries, CancellationToken cancellationToken = default)
        => throw new NotSupportedException("MimironDb2 is a read-only provider.");
}
