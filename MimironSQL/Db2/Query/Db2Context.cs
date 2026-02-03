using MimironSQL.Db2.Model;
using MimironSQL.Db2.Schema;
using MimironSQL.Formats;
using MimironSQL.Providers;

using System.Collections.Concurrent;
using System.Reflection;
using System.Threading;

namespace MimironSQL.Db2.Query;

public abstract class Db2Context
{
    private static readonly Type Db2TableOpenGenericType = typeof(Db2Table<>);
    private static readonly ConcurrentDictionary<Type, Func<Db2Context, string, IDb2Table>> OpenTableDelegates = new();
    private static readonly ConcurrentDictionary<PropertyInfo, Action<Db2Context, IDb2Table>> TablePropertySetters = new();
    private static readonly ConcurrentDictionary<(Type EntityType, Type RowType), Func<Db2Context, string, Db2TableSchema, IDb2File, IDb2Table>> CreateTypedTableDelegates = new();

    private readonly SchemaMapper schemaMapper;
    private readonly IDb2StreamProvider db2StreamProvider;
    private readonly Db2ContextQueryProvider queryProvider;
    private readonly Dictionary<string, (IDb2File File, Db2TableSchema Schema)> cache = new(StringComparer.OrdinalIgnoreCase);

    private readonly IDb2Format format;

    private Db2Model? model;
    private readonly Lock modelCreationLock = new();

    protected Db2Context(IDbdProvider dbdProvider, IDb2StreamProvider db2StreamProvider, IDb2Format format)
    {
        schemaMapper = new SchemaMapper(dbdProvider);
        this.db2StreamProvider = db2StreamProvider;
        queryProvider = new Db2ContextQueryProvider(this);

        this.format = format;
    }

    public void EnsureModelCreated()
    {
        if (model is not null)
            return;

        using var _ = modelCreationLock.EnterScope();
        model ??= BuildModel();
        InitializeTableProperties(model);
    }

    public virtual void OnModelCreating(Db2ModelBuilder modelBuilder)
    {
    }

    internal Db2Model Model
        => model ?? throw new InvalidOperationException("The context model has not been created. Call EnsureModelCreated() before querying.");

    private void ThrowIfModelNotCreated()
    {
        if (model is null)
            throw new InvalidOperationException("The context model has not been created. Call EnsureModelCreated() before querying.");
    }

    internal (IDb2File File, Db2TableSchema Schema) GetOrOpenTableRaw(string tableName)
    {
        if (model is null && !modelCreationLock.IsHeldByCurrentThread)
            throw new InvalidOperationException("The context model has not been created. Call EnsureModelCreated() before querying.");

        if (cache.TryGetValue(tableName, out var cached))
            return cached;

        using var stream = db2StreamProvider.OpenDb2Stream(tableName);
        var file = format.OpenFile(stream);
        var layout = format.GetLayout(file);
        var schema = schemaMapper.GetSchema(tableName, layout);
        cache[tableName] = (file, schema);
        return (file, schema);
    }

    internal (IDb2File<TRow> File, Db2TableSchema Schema) GetOrOpenTableRawTyped<TRow>(string tableName)
        where TRow : struct
    {
        var (file, schema) = GetOrOpenTableRaw(tableName);
        return ((IDb2File<TRow>)file, schema);
    }

    private void InitializeTableProperties(Db2Model db2Model)
    {
        var derivedType = GetType();
        foreach (var p in derivedType.GetProperties(BindingFlags.Instance | BindingFlags.Public))
        {
            if (p.PropertyType is not { IsGenericType: true } pt)
                continue;

            if (pt.GetGenericTypeDefinition() != Db2TableOpenGenericType)
                continue;

            if (p.SetMethod is not { IsPublic: true })
                continue;

            var entityType = pt.GetGenericArguments()[0];
            var tableName = db2Model.TryGetEntityType(entityType, out var configured)
                ? configured.TableName
                : entityType.Name;

            var openTable = OpenTableDelegates.GetOrAdd(entityType, static entityType =>
            {
                var factoryMethod = typeof(Db2Context)
                    .GetMethod(nameof(OpenTableByEntity), BindingFlags.Static | BindingFlags.NonPublic)!;

                var generic = factoryMethod.MakeGenericMethod(entityType);
                return generic.CreateDelegate<Func<Db2Context, string, IDb2Table>>();
            });

            var setter = TablePropertySetters.GetOrAdd(p, static p =>
            {
                var ctx = System.Linq.Expressions.Expression.Parameter(typeof(Db2Context), "ctx");
                var value = System.Linq.Expressions.Expression.Parameter(typeof(IDb2Table), "value");

                var declaring = System.Linq.Expressions.Expression.Convert(ctx, p.DeclaringType!);
                var typedValue = System.Linq.Expressions.Expression.Convert(value, p.PropertyType);
                var set = System.Linq.Expressions.Expression.Call(declaring, p.SetMethod!, typedValue);
                return System.Linq.Expressions.Expression.Lambda<Action<Db2Context, IDb2Table>>(set, ctx, value).Compile();
            });

            var table = openTable(this, tableName);
            setter(this, table);
        }
    }

    private Db2Model BuildModel()
    {
        var builder = new Db2ModelBuilder();
        builder.ApplyTablePropertyConventions(GetType());
        OnModelCreating(builder);

        builder.ApplyAttributeNavigationConventions();

        builder.ApplySchemaNavigationConventions(tableName => GetOrOpenTableRaw(tableName).Schema);
        return builder.Build(tableName => GetOrOpenTableRaw(tableName).Schema);
    }

    private static Db2Table<TEntity> OpenTableByEntity<TEntity>(Db2Context context, string tableName)
        => context.OpenTableGeneric<TEntity>(tableName);

    private Db2Table<T> OpenTableGeneric<T>(string tableName)
    {
        var (file, schema) = GetOrOpenTableRaw(tableName);

        var create = CreateTypedTableDelegates.GetOrAdd((typeof(T), file.RowType), static types =>
        {
            var factoryMethod = typeof(Db2Context)
                .GetMethod(nameof(CreateTypedTableByTypes), BindingFlags.Static | BindingFlags.NonPublic)!;

            var generic = factoryMethod.MakeGenericMethod(types.EntityType, types.RowType);
            return generic.CreateDelegate<Func<Db2Context, string, Db2TableSchema, IDb2File, IDb2Table>>();
        });

        return (Db2Table<T>)create(this, tableName, schema, file);
    }

    private static Db2Table<TEntity> CreateTypedTableByTypes<TEntity, TRow>(Db2Context context, string tableName, Db2TableSchema schema, IDb2File file)
        where TRow : struct
        => context.CreateTypedTable<TEntity, TRow>(tableName, schema, file);

    private Db2Table<TEntity> CreateTypedTable<TEntity, TRow>(string tableName, Db2TableSchema schema, IDb2File file)
        where TRow : struct
    {
        ThrowIfModelNotCreated();

        var entityType = model!.GetEntityType(typeof(TEntity));
        if (!entityType.TableName.Equals(tableName, StringComparison.OrdinalIgnoreCase))
            entityType = entityType.WithSchema(tableName, schema);

        return new Db2Table<TEntity, TRow>(tableName, schema, entityType, queryProvider, (IDb2File<TRow>)file);
    }

    protected Db2Table<T> Table<T>(string? tableName = null)
    {
        ThrowIfModelNotCreated();
        return tableName switch
        {
            not null => OpenTableGeneric<T>(tableName),
            _ => model!.TryGetEntityType(typeof(T), out var configured)
                                ? OpenTableGeneric<T>(configured.TableName)
                                : OpenTableGeneric<T>(typeof(T).Name),
        };
    }
}

