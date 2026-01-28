using MimironSQL.Db2.Model;
using MimironSQL.Db2.Schema;
using MimironSQL.Db2.Wdc5;
using MimironSQL.Formats;
using MimironSQL.Formats.Wdc5;
using MimironSQL.Providers;

using System.Reflection;

namespace MimironSQL.Db2.Query;

public abstract class Db2Context
{
    private static readonly Type Db2TableOpenGenericType = typeof(Db2Table<>);

    private readonly SchemaMapper _schemaMapper;
    private readonly IDb2StreamProvider _db2StreamProvider;
    private readonly Db2ContextQueryProvider _queryProvider;
    private readonly Dictionary<string, (Wdc5File File, Db2TableSchema Schema)> _cache = new(StringComparer.OrdinalIgnoreCase);

    private Db2Model? _model;
    private IDb2Format? _format;
    private bool _isInitialized;
    private bool _isInitializing;

    protected Db2Context(IDbdProvider dbdProvider, IDb2StreamProvider db2StreamProvider)
        : this(dbdProvider, db2StreamProvider, registerFormats: null, deferInitialization: false)
    {
    }

    protected Db2Context(IDbdProvider dbdProvider, IDb2StreamProvider db2StreamProvider, bool deferInitialization)
        : this(dbdProvider, db2StreamProvider, registerFormats: null, deferInitialization)
    {
    }

    protected Db2Context(IDbdProvider dbdProvider, IDb2StreamProvider db2StreamProvider, Action<Db2FormatRegistry> registerFormats)
        : this(dbdProvider, db2StreamProvider, registerFormats, deferInitialization: false)
    {
    }

    private Db2Context(IDbdProvider dbdProvider, IDb2StreamProvider db2StreamProvider, Action<Db2FormatRegistry>? registerFormats, bool deferInitialization)
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

    internal (Wdc5File File, Db2TableSchema Schema) GetOrOpenTableRaw(string tableName)
    {
        EnsureInitialized();

        if (_cache.TryGetValue(tableName, out var cached))
            return cached;

        using var stream = _db2StreamProvider.OpenDb2Stream(tableName);
        var opened = _format!.OpenFile(stream);
        if (opened is not Wdc5File file)
            throw new NotSupportedException("Only WDC5 files are currently supported by the built-in format.");

        var layout = _format.GetLayout(file);
        var schema = _schemaMapper.GetSchema(tableName, layout);
        _cache[tableName] = (file, schema);
        return (file, schema);
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

            var table = OpenTable(entityType, tableName);
            p.SetValue(this, table);
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

    private object OpenTable(Type entityType, string tableName)
    {
        ArgumentNullException.ThrowIfNull(entityType);
        ArgumentNullException.ThrowIfNull(tableName);

        var m = typeof(Db2Context).GetMethod(nameof(OpenTableGeneric), BindingFlags.Instance | BindingFlags.NonPublic)!;
        return m.MakeGenericMethod(entityType).Invoke(this, [tableName])!;
    }

    private Db2Table<T> OpenTableGeneric<T>(string tableName)
    {
        var (file, schema) = GetOrOpenTableRaw(tableName);

        return new Db2Table<T>(tableName, schema, _queryProvider, name => GetOrOpenTableRaw(name).File);
    }

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

