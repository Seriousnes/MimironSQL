using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;

using MimironSQL.EntityFrameworkCore.Storage;

namespace MimironSQL.EntityFrameworkCore.Db2.Model;

internal sealed class Db2ModelBindingProvider(
    ICurrentDbContext currentDbContext,
    IMimironDb2Store store) : IDb2ModelBinding
{
    private readonly DbContext _context = currentDbContext?.Context ?? throw new ArgumentNullException(nameof(currentDbContext));
    private readonly IMimironDb2Store _store = store ?? throw new ArgumentNullException(nameof(store));

    private Db2ModelBinding? _binding;

    public Db2ModelBinding GetBinding()
        => _binding ??= new Db2ModelBinding(
            _context.Model,
            tableName => _store.GetSchema(tableName));
}
