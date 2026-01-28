using MimironSQL.Db2.Schema;
using MimironSQL.Db2.Wdc5;
using MimironSQL.Formats;

using System.Collections;
using System.Linq.Expressions;
using System.Numerics;

namespace MimironSQL.Db2.Query;

public sealed class Db2Table<T> : IQueryable<T>
{
    internal string TableName { get; }
    public Db2TableSchema Schema { get; }

    internal Wdc5File File => (Wdc5File)_fileResolver(TableName);

    private readonly IQueryProvider _provider;
    private readonly Db2EntityMaterializer<T> _materializer;
    private readonly Func<string, IDb2File> _fileResolver;

    public Type ElementType => typeof(T);
    public Expression Expression { get; }
    public IQueryProvider Provider => _provider;

    internal Db2Table(string tableName, Db2TableSchema schema, IQueryProvider provider, Func<string, IDb2File> fileResolver)
    {
        TableName = tableName;
        Schema = schema;
        _provider = provider;
        _materializer = new Db2EntityMaterializer<T>(schema);
        _fileResolver = fileResolver;
        Expression = Expression.Constant(this);
    }

    public IEnumerator<T> GetEnumerator()
        => ((IEnumerable<T>)_provider.Execute<IEnumerable<T>>(Expression)).GetEnumerator();

    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

    public T? Find<TId>(TId id)
        where TId : IBinaryInteger<TId>
    {
        if (!typeof(Db2Entity<TId>).IsAssignableFrom(typeof(T)))
            throw new NotSupportedException($"Entity type {typeof(T).FullName} must derive from {typeof(Db2Entity<TId>).FullName} to use Find with key type {typeof(TId).FullName}.");

        var file = (Wdc5File)_fileResolver(TableName);
        if (!file.TryGetRowById(id, out var row))
            return default;

        return _materializer.Materialize(row);
    }
}
