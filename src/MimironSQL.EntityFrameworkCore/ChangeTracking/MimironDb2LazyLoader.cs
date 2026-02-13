using System.Collections.Concurrent;
using System.Reflection;
using System.Runtime.CompilerServices;

using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Metadata;

using MimironSQL.EntityFrameworkCore.Db2.Model;
using MimironSQL.EntityFrameworkCore.Db2.Query;
using MimironSQL.EntityFrameworkCore.Db2.Schema;
using MimironSQL.EntityFrameworkCore.Storage;
using MimironSQL.Formats;

namespace MimironSQL.EntityFrameworkCore.ChangeTracking;

internal sealed class MimironDb2LazyLoader(
    ICurrentDbContext currentDbContext,
    IMimironDb2Store store,
    IDb2ModelBinding modelBinding) : ILazyLoader
{
    private static readonly ConcurrentDictionary<(Type EntityClrType, Type RowType), Action<MimironDb2LazyLoader, object, PropertyInfo>> LoadDelegates = new();

    private readonly DbContext _context = currentDbContext?.Context ?? throw new ArgumentNullException(nameof(currentDbContext));
    private readonly IMimironDb2Store _store = store ?? throw new ArgumentNullException(nameof(store));
    private readonly IDb2ModelBinding _modelBinding = modelBinding ?? throw new ArgumentNullException(nameof(modelBinding));

    private readonly IDb2EntityFactory _entityFactory = new EfLazyLoadingProxyDb2EntityFactory(
        currentDbContext?.Context ?? throw new ArgumentNullException(nameof(currentDbContext)),
        new ReflectionDb2EntityFactory());

    private readonly ConditionalWeakTable<object, HashSet<string>> _loadedByEntity = [];

    public void SetLoaded(object entity, string navigationName, bool loaded)
    {
        ArgumentNullException.ThrowIfNull(entity);
        ArgumentNullException.ThrowIfNull(navigationName);

        if (loaded)
        {
            var set = _loadedByEntity.GetOrCreateValue(entity);
            lock (set)
                set.Add(navigationName);

            return;
        }

        if (_loadedByEntity.TryGetValue(entity, out var existing))
        {
            lock (existing)
                existing.Remove(navigationName);
        }
    }

    public bool IsLoaded(object entity, string navigationName)
    {
        ArgumentNullException.ThrowIfNull(entity);
        ArgumentNullException.ThrowIfNull(navigationName);

        if (!_loadedByEntity.TryGetValue(entity, out var set))
            return false;

        lock (set)
            return set.Contains(navigationName);
    }

    public void Load(object entity, string navigationName)
    {
        ArgumentNullException.ThrowIfNull(entity);
        ArgumentNullException.ThrowIfNull(navigationName);

        if (IsLoaded(entity, navigationName))
            return;

        var efEntityType = FindEfEntityType(entity.GetType());
        if (efEntityType is null)
            return;

        var navigation = (INavigationBase?)efEntityType.FindNavigation(navigationName)
            ?? efEntityType.FindSkipNavigation(navigationName);

        if (navigation?.PropertyInfo is null)
            return;

        SetLoaded(entity, navigationName, loaded: true);

        // This provider currently uses RowHandle for all shipped formats.
        // Avoid opening DB2 files here (and holding streams) just to discover row type.
        var rowType = typeof(RowHandle);
        var loader = LoadDelegates.GetOrAdd((efEntityType.ClrType, rowType), static key =>
        {
            var method = typeof(MimironDb2LazyLoader)
                .GetMethod(nameof(LoadTyped), BindingFlags.NonPublic | BindingFlags.Instance)!
                .MakeGenericMethod(key.EntityClrType, key.RowType);

            return method.CreateDelegate<Action<MimironDb2LazyLoader, object, PropertyInfo>>();
        });

        loader(this, entity, navigation.PropertyInfo);
    }

    public Task LoadAsync(object entity, CancellationToken cancellationToken, string navigationName)
    {
        cancellationToken.ThrowIfCancellationRequested();
        Load(entity, navigationName);
        return Task.CompletedTask;
    }

    public void Dispose()
    {
    }

    private IEntityType? FindEfEntityType(Type runtimeType)
    {
        var current = runtimeType;
        while (current is not null)
        {
            var et = _context.Model.FindEntityType(current);
            if (et is not null)
                return et;

            current = current.BaseType;
        }

        return null;
    }

    private void LoadTyped<TEntity, TRow>(object entity, PropertyInfo navigationProperty)
        where TEntity : class
        where TRow : struct, IRowHandle
    {
        var model = _modelBinding.GetBinding();

        (IDb2File<TRow> File, Db2TableSchema Schema) TableResolver(string name)
            => _store.OpenTableWithSchema<TRow>(name);

        var typedEntity = (TEntity)entity;

        _ = Db2IncludeChainExecutor.Apply(
            source: [typedEntity],
            model: model,
            tableResolver: TableResolver,
            members: [navigationProperty],
            entityFactory: _entityFactory);
    }
}
