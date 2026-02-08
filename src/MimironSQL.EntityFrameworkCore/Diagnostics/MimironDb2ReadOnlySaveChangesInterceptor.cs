using Microsoft.EntityFrameworkCore.Diagnostics;

namespace MimironSQL.EntityFrameworkCore.Diagnostics;

internal sealed class MimironDb2ReadOnlySaveChangesInterceptor : SaveChangesInterceptor
{
    public override InterceptionResult<int> SavingChanges(
        DbContextEventData eventData,
        InterceptionResult<int> result)
        => throw new NotSupportedException("MimironDb2 is a read-only provider. SaveChanges is not supported.");

    public override ValueTask<InterceptionResult<int>> SavingChangesAsync(
        DbContextEventData eventData,
        InterceptionResult<int> result,
        CancellationToken cancellationToken = default)
        => throw new NotSupportedException("MimironDb2 is a read-only provider. SaveChangesAsync is not supported.");
}
