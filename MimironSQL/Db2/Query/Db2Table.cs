using MimironSQL.Db2.Schema;
using MimironSQL.Db2.Wdc5;

using System.Collections;
using System.Linq.Expressions;
using System.Numerics;

namespace MimironSQL.Db2.Query;

public sealed class Db2Table<T> : IQueryable<T>, IDb2Table
{
    internal Wdc5File File { get; }
    public Db2TableSchema Schema { get; }

    private readonly IQueryProvider _provider;
    private readonly Db2EntityMaterializer<T> _materializer;

    public Type ElementType => typeof(T);
    public Expression Expression { get; }
    public IQueryProvider Provider => _provider;

    Type IDb2Table.EntityType => typeof(T);
    Wdc5File IDb2Table.File => File;
    Db2TableSchema IDb2Table.Schema => Schema;

    internal Db2Table(Wdc5File file, Db2TableSchema schema, IQueryProvider provider)
    {
        File = file;
        Schema = schema;
        _provider = provider;
        _materializer = new Db2EntityMaterializer<T>(schema);
        Expression = Expression.Constant(this);
    }

    public IEnumerator<T> GetEnumerator()
        => ((IEnumerable<T>)_provider.Execute<IEnumerable<T>>(Expression)).GetEnumerator();

    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

    public T? Find<TId>(TId id)
        where TId : IBinaryInteger<TId>
    {
        if (!typeof(Wdc5Entity<TId>).IsAssignableFrom(typeof(T)))
            throw new NotSupportedException($"Entity type {typeof(T).FullName} must derive from {typeof(Wdc5Entity<TId>).FullName} to use Find with key type {typeof(TId).FullName}.");

        if (!File.TryGetRowById(id, out var row))
            return default;

        return _materializer.Materialize(row);
    }
}
