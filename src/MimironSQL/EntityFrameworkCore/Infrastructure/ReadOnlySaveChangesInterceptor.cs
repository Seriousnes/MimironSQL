using Microsoft.EntityFrameworkCore.Diagnostics;

namespace MimironSQL.EntityFrameworkCore.Infrastructure;

public sealed class ReadOnlySaveChangesInterceptor : SaveChangesInterceptor
{
    public static readonly ReadOnlySaveChangesInterceptor Instance = new();

    private ReadOnlySaveChangesInterceptor()
    {
    }

    public override InterceptionResult<int> SavingChanges(
        DbContextEventData eventData,
        InterceptionResult<int> result)
        => throw new NotSupportedException("Mimiron DB2 is read-only; SaveChanges is not supported.");

    public override ValueTask<InterceptionResult<int>> SavingChangesAsync(
        DbContextEventData eventData,
        InterceptionResult<int> result,
        CancellationToken cancellationToken = default)
        => throw new NotSupportedException("Mimiron DB2 is read-only; SaveChangesAsync is not supported.");
}
