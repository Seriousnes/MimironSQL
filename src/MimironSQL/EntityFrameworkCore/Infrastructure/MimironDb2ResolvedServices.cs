using System.Collections.Generic;

using MimironSQL.Formats;
using MimironSQL.Providers;

namespace MimironSQL.EntityFrameworkCore.Infrastructure;

internal sealed class MimironDb2ResolvedServices(
    IDb2StreamProvider db2StreamProvider,
    IDbdProvider dbdProvider,
    ITactKeyProvider tactKeyProvider,
    IDb2Format format)
{
    public IDb2StreamProvider Db2StreamProvider { get; } = db2StreamProvider;
    public IDbdProvider DbdProvider { get; } = dbdProvider;
    public ITactKeyProvider TactKeyProvider { get; } = tactKeyProvider;
    public IDb2Format Format { get; } = format;

    public static MimironDb2ResolvedServices Resolve(IServiceProvider serviceProvider)
    {
        ArgumentNullException.ThrowIfNull(serviceProvider);

        var db2StreamProvider = (IDb2StreamProvider?)serviceProvider.GetService(typeof(IDb2StreamProvider))
            ?? throw new InvalidOperationException($"Missing required service: {typeof(IDb2StreamProvider).FullName}");

        var dbdProvider = (IDbdProvider?)serviceProvider.GetService(typeof(IDbdProvider))
            ?? throw new InvalidOperationException($"Missing required service: {typeof(IDbdProvider).FullName}");

        var tactKeyProvider = (ITactKeyProvider?)serviceProvider.GetService(typeof(ITactKeyProvider))
            ?? throw new InvalidOperationException($"Missing required service: {typeof(ITactKeyProvider).FullName}");

        var formats = (IEnumerable<IDb2Format>?)serviceProvider.GetService(typeof(IEnumerable<IDb2Format>)) ?? [];
        IDb2Format? selected = null;
        var count = 0;

        foreach (var f in formats)
        {
            count++;
            selected = f;

            if (count > 1)
                break;
        }

        if (count == 0)
        {
            throw new InvalidOperationException(
                $"Missing required service: {typeof(IDb2Format).FullName}. Register exactly one format (e.g. services.AddSingleton<IDb2Format, Wdc5Format>()).");
        }

        if (count > 1)
            throw new InvalidOperationException($"Multiple {typeof(IDb2Format).FullName} registrations found; register exactly one.");

        return new MimironDb2ResolvedServices(db2StreamProvider, dbdProvider, tactKeyProvider, selected!);
    }
}
