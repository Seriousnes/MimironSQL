using System.Runtime.CompilerServices;

using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Metadata;

using MimironSQL.EntityFrameworkCore.Storage;

namespace MimironSQL.EntityFrameworkCore.Db2.Model;

internal sealed class Db2ModelBindingProvider(
    ICurrentDbContext currentDbContext,
    IMimironDb2Store store) : IDb2ModelBinding
{
    private static readonly ConditionalWeakTable<IModel, Db2ModelBinding> Cache = new();

    public Db2ModelBinding GetBinding()
        => Cache.GetValue(
            currentDbContext.Context.Model,
            model => new Db2ModelBinding(model, store.GetSchema));
}
