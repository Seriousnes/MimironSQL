using MimironSQL.Db2.Schema;
using MimironSQL.Db2.Wdc5;

using System.Collections;
using System.Linq.Expressions;
using System.Numerics;

namespace MimironSQL.Db2.Query;

public sealed class Db2Table<T> : IQueryable<T>
{
    internal Wdc5File File { get; }
    public Db2TableSchema Schema { get; }

    private readonly Db2QueryProvider<T> _provider;

    public Type ElementType => typeof(T);
    public Expression Expression { get; }
    public IQueryProvider Provider => _provider;

    internal Db2Table(Wdc5File file, Db2TableSchema schema)
    {
        File = file;
        Schema = schema;
        _provider = new Db2QueryProvider<T>(file, schema);
        Expression = Expression.Constant(this);
    }

    public IEnumerator<T> GetEnumerator()
        => ((IEnumerable<T>)_provider.Execute<IEnumerable<T>>(Expression)).GetEnumerator();

    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

    public T? Find<TId>(TId id)
        where TId : IBinaryInteger<TId>
    {
        if (!typeof(global::MimironSQL.Db2.Wdc5Entity<TId>).IsAssignableFrom(typeof(T)))
            throw new NotSupportedException($"Entity type {typeof(T).FullName} must derive from {typeof(global::MimironSQL.Db2.Wdc5Entity<TId>).FullName} to use Find with key type {typeof(TId).FullName}.");

        if (!File.TryGetRowById(id, out var row))
            return default;

        return _provider.Materialize(row);
    }
}
