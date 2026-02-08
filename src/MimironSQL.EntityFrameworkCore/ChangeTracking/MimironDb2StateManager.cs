using Microsoft.EntityFrameworkCore.ChangeTracking.Internal;

namespace MimironSQL.EntityFrameworkCore.ChangeTracking;

internal sealed class MimironDb2StateManager(StateManagerDependencies dependencies) : StateManager(dependencies)
{
    public override int SaveChanges(bool acceptAllChangesOnSuccess)
        => throw new NotSupportedException("MimironDb2 is a read-only provider. SaveChanges is not supported.");

    public override Task<int> SaveChangesAsync(
        bool acceptAllChangesOnSuccess,
        CancellationToken cancellationToken = default)
        => throw new NotSupportedException("MimironDb2 is a read-only provider. SaveChangesAsync is not supported.");
}
