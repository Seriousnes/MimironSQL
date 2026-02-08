using System.Linq.Expressions;

using MimironSQL.Db2;
using MimironSQL.Db2.Model;
using MimironSQL.Db2.Query;
using MimironSQL.Db2.Schema;
using MimironSQL.Formats;

using NSubstitute;

using Shouldly;

namespace MimironSQL.EntityFrameworkCore.Tests;

public sealed class Db2QueryProviderIQueryProviderSurfaceTests
{
    private sealed class Entity
    {
        public int Id { get; init; }
    }

    [Fact]
    public void CreateQuery_non_generic_creates_Db2Queryable_for_element_type()
    {
        var provider = CreateProvider();

        var expression = Expression.Constant(Enumerable.Empty<int>().AsQueryable());

        var query = provider.CreateQuery(expression);

        query.ShouldBeOfType<Db2Queryable<int>>();
    }

    [Fact]
    public void CreateQuery_generic_creates_Db2Queryable_for_element_type()
    {
        var provider = CreateProvider();

        var expression = Expression.Constant(Enumerable.Empty<int>().AsQueryable());

        var query = provider.CreateQuery<int>(expression);

        query.ShouldBeOfType<Db2Queryable<int>>();
    }

    [Fact]
    public void Execute_non_generic_throws_for_scalar_expressions()
    {
        var provider = CreateProvider();

        var ex = Should.Throw<NotSupportedException>(() => provider.Execute(Expression.Constant(1)));
        ex.Message.ShouldContain("Use Execute<TResult>");
    }

    private static Db2QueryProvider<Entity, RowHandle> CreateProvider()
    {
        var builder = new Db2ModelBuilder();
        builder.Entity<Entity>().HasKey(x => x.Id);
        var model = builder.Build(SchemaResolver);

        var file = Substitute.For<IDb2File<RowHandle>>();

        (IDb2File<RowHandle> File, Db2TableSchema Schema) TableResolver(string tableName)
            => (file, SchemaResolver(tableName));

        var entityFactory = new ReflectionDb2EntityFactory();

        return new Db2QueryProvider<Entity, RowHandle>(
            file,
            model,
            TableResolver,
            entityFactory);
    }

    private static Db2TableSchema SchemaResolver(string tableName)
    {
        var id = new Db2FieldSchema(
            Name: "ID",
            ValueType: Db2ValueType.Int64,
            ColumnStartIndex: 0,
            ElementCount: 1,
            IsVerified: true,
            IsVirtual: false,
            IsId: true,
            IsRelation: false,
            ReferencedTableName: null);

        return new Db2TableSchema(
            tableName,
            layoutHash: 0,
            physicalColumnCount: 1,
            fields: [id]);
    }
}
