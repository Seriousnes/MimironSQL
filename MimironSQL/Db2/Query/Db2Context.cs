using MimironSQL.Db2.Model;
using MimironSQL.Db2.Schema;
using MimironSQL.Formats;
using MimironSQL.Formats.Wdc5;
using MimironSQL.Providers;

using System.Collections.Concurrent;
using System.Reflection;

namespace MimironSQL.Db2.Query;

public abstract class Db2Context
{
    private static readonly Type Db2TableOpenGenericType = typeof(Db2Table<>);
    private static readonly ConcurrentDictionary<Type, Func<Db2Context, string, IDb2Table>> OpenTableDelegates = new();
    private static readonly ConcurrentDictionary<PropertyInfo, Action<Db2Context, IDb2Table>> TablePropertySetters = new();
    private static readonly ConcurrentDictionary<(Type EntityType, Type RowType), Func<Db2Context, string, Db2TableSchema, IDb2File, IDb2Table>> CreateTypedTableDelegates = new();

    private readonly SchemaMapper _schemaMapper;
    private readonly IDb2StreamProvider _db2StreamProvider;
    private readonly Db2ContextQueryProvider _queryProvider;
    private readonly Dictionary<string, (IDb2File File, Db2TableSchema Schema)> _cache = new(StringComparer.OrdinalIgnoreCase);

    private Db2Model? _model;
    private IDb2Format? _format;
    private bool _isInitialized;
    private bool _isInitializing;

    protected Db2Context(IDbdProvider dbdProvider, IDb2StreamProvider db2StreamProvider)
        : this(dbdProvider, db2StreamProvider, registerFormats: null, deferInitialization: false)
    {
    }

    private Db2Context(IDbdProvider dbdProvider, IDb2StreamProvider db2StreamProvider, Action<Db2FormatRegistry>? registerFormats, bool deferInitialization = true)
    {
        _schemaMapper = new SchemaMapper(dbdProvider);
        _db2StreamProvider = db2StreamProvider;
        _queryProvider = new Db2ContextQueryProvider(this);

        if (!deferInitialization)
            RegisterFormatInternal(registerFormats ?? Wdc5Format.Register);
    }

    protected virtual void OnModelCreating(Db2ModelBuilder modelBuilder)
    {
    }

    internal Db2Model Model
    {
        get
        {
            EnsureInitialized();
            return _model!;
        }
    }

    public void RegisterFormat(params IDb2Format[] formats)
    {
        ArgumentNullException.ThrowIfNull(formats);
        RegisterFormat(registry =>
        {
            foreach (var format in formats)
                registry.Register(format);
        });
    }

    public void RegisterFormat(Action<Db2FormatRegistry> registerFormats)
    {
        ArgumentNullException.ThrowIfNull(registerFormats);

        if (_isInitialized)
            throw new InvalidOperationException("Format registration must occur before the context is initialized.");

        RegisterFormatInternal(registerFormats);
    }

    private void RegisterFormatInternal(Action<Db2FormatRegistry> registerFormats)
    {
        if (_format is not null)
            throw new InvalidOperationException("A format has already been registered for this context instance.");

        var registry = new Db2FormatRegistry();
        registerFormats(registry);

        if (registry.Formats is not { Count: 1 })
            throw new InvalidOperationException($"Expected exactly one registered format for this context instance, but got {registry.Formats.Count}.");

        _format = registry.Formats[0];
        InitializeAfterFormatRegistration();
    }

    private void EnsureInitialized()
    {
        if (_isInitialized)
            return;

        if (_isInitializing)
            return;

        if (_format is null)
        {
            RegisterFormatInternal(Wdc5Format.Register);
            return;
        }

        InitializeAfterFormatRegistration();
    }

    private void InitializeAfterFormatRegistration()
    {
        if (_isInitialized)
            return;

        if (_isInitializing)
            return;

        _isInitializing = true;

        _model = BuildModel();
        InitializeTableProperties();
        _isInitialized = true;
        _isInitializing = false;
    }

    internal (IDb2File File, Db2TableSchema Schema) GetOrOpenTableRaw(string tableName)
    {
        EnsureInitialized();

        if (_cache.TryGetValue(tableName, out var cached))
            return cached;

        using var stream = _db2StreamProvider.OpenDb2Stream(tableName);
        var file = _format!.OpenFile(stream);
        var layout = _format.GetLayout(file);
        var schema = _schemaMapper.GetSchema(tableName, layout);
        _cache[tableName] = (file, schema);
        return (file, schema);
    }

    internal (IDb2File<TRow> File, Db2TableSchema Schema) GetOrOpenTableRawTyped<TRow>(string tableName)
        where TRow : struct
    {
        var (file, schema) = GetOrOpenTableRaw(tableName);
        return ((IDb2File<TRow>)file, schema);
    }

    private void InitializeTableProperties()
    {
        var model = _model!;
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
            var tableName = model.TryGetEntityType(entityType, out var configured)
                ? configured.TableName
                : entityType.Name;

            var openTable = OpenTableDelegates.GetOrAdd(entityType, static entityType =>
            {
                var factoryMethod = typeof(Db2Context)
                    .GetMethod(nameof(OpenTableByEntity), BindingFlags.Static | BindingFlags.NonPublic)!;

                var generic = factoryMethod.MakeGenericMethod(entityType);
                return (Func<Db2Context, string, IDb2Table>)generic.CreateDelegate(typeof(Func<Db2Context, string, IDb2Table>));
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

        builder.ApplySchemaNavigationConventions(tableName => GetOrOpenTableRaw(tableName).Schema);
        return builder.Build(tableName => GetOrOpenTableRaw(tableName).Schema);
    }

    private static IDb2Table OpenTableByEntity<TEntity>(Db2Context context, string tableName)
        => context.OpenTableGeneric<TEntity>(tableName);

    private Db2Table<T> OpenTableGeneric<T>(string tableName)
    {
        var (file, schema) = GetOrOpenTableRaw(tableName);

        var create = CreateTypedTableDelegates.GetOrAdd((typeof(T), file.RowType), static types =>
        {
            var factoryMethod = typeof(Db2Context)
                .GetMethod(nameof(CreateTypedTableByTypes), BindingFlags.Static | BindingFlags.NonPublic)!;

            var generic = factoryMethod.MakeGenericMethod(types.EntityType, types.RowType);
            return (Func<Db2Context, string, Db2TableSchema, IDb2File, IDb2Table>)generic.CreateDelegate(
                typeof(Func<Db2Context, string, Db2TableSchema, IDb2File, IDb2Table>));
        });

        return (Db2Table<T>)create(this, tableName, schema, file);
    }

    private static IDb2Table CreateTypedTableByTypes<TEntity, TRow>(Db2Context context, string tableName, Db2TableSchema schema, IDb2File file)
        where TRow : struct
        => context.CreateTypedTable<TEntity, TRow>(tableName, schema, file);

    private Db2Table<TEntity> CreateTypedTable<TEntity, TRow>(string tableName, Db2TableSchema schema, IDb2File file)
        where TRow : struct
        => new Db2Table<TEntity, TRow>(tableName, schema, _queryProvider, (IDb2File<TRow>)file);

    protected Db2Table<T> Table<T>(string? tableName = null)
    {
        EnsureInitialized();
        if (tableName is not null)
            return OpenTableGeneric<T>(tableName);

        return _model!.TryGetEntityType(typeof(T), out var configured)
            ? OpenTableGeneric<T>(configured.TableName)
            : OpenTableGeneric<T>(typeof(T).Name);
    }
}

