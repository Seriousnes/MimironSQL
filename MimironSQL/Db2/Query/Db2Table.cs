using MimironSQL.Db2.Schema;
using MimironSQL.Formats;

using System.Collections;
using System.Linq.Expressions;
using System.Numerics;

namespace MimironSQL.Db2.Query;

internal interface IDb2Table
{
    Type EntityType { get; }
    string TableName { get; }
}

public class Db2Table<T> : IQueryable<T>, IDb2Table
{
    internal string TableName { get; }
    public Db2TableSchema Schema { get; }

    private readonly IQueryProvider _provider;

    public Type ElementType => typeof(T);
    public Expression Expression { get; }
    public IQueryProvider Provider => _provider;

    Type IDb2Table.EntityType => typeof(T);
    string IDb2Table.TableName => TableName;

    internal Db2Table(string tableName, Db2TableSchema schema, IQueryProvider provider)
    {
        TableName = tableName;
        Schema = schema;
        _provider = provider;
        Expression = Expression.Constant(this);
    }

    public IEnumerator<T> GetEnumerator()
        => ((IEnumerable<T>)_provider.Execute<IEnumerable<T>>(Expression)).GetEnumerator();

    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

    public virtual T? Find<TId>(TId id)
        where TId : IBinaryInteger<TId>
    {
        throw new NotSupportedException("Find is not supported for this table instance.");
    }
}

internal sealed class Db2Table<T, TRow> : Db2Table<T>
    where TRow : struct, IDb2Row
{
    private readonly IDb2File<TRow> _file;
    private readonly Db2EntityMaterializer<T, TRow> _materializer;

    internal Db2Table(string tableName, Db2TableSchema schema, IQueryProvider provider, IDb2File<TRow> file)
        : base(tableName, schema, provider)
    {
        _file = file;
        _materializer = new Db2EntityMaterializer<T, TRow>(schema);
    }

    public override T? Find<TId>(TId id)
    {
        if (!typeof(Db2Entity<TId>).IsAssignableFrom(typeof(T)))
            throw new NotSupportedException($"Entity type {typeof(T).FullName} must derive from {typeof(Db2Entity<TId>).FullName} to use Find with key type {typeof(TId).FullName}.");

        var key = int.CreateChecked(id);

        if (!_file.TryGetRowById(key, out var row))
            return default;

        return _materializer.Materialize(row);
    }
}
