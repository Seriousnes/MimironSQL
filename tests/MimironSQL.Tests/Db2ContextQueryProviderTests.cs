using System.Linq.Expressions;

using MimironSQL.Db2.Query;
using MimironSQL.Formats;

using NSubstitute;

using Shouldly;

namespace MimironSQL.Tests;

public sealed class Db2ContextQueryProviderTests
{
    private sealed class NoOpContext : Db2Context
    {
        public NoOpContext()
            : base(
                dbdProvider: Substitute.For<MimironSQL.Providers.IDbdProvider>(),
                db2StreamProvider: Substitute.For<MimironSQL.Providers.IDb2StreamProvider>(),
                format: Substitute.For<IDb2Format>())
        {
        }
    }

    [Fact]
    public void CreateQuery_non_generic_creates_Db2Queryable_for_element_type()
    {
        var provider = new Db2ContextQueryProvider(new NoOpContext());

        var expression = Expression.Constant(Enumerable.Empty<int>().AsQueryable());

        var query = provider.CreateQuery(expression);

        query.ShouldBeOfType<Db2Queryable<int>>();
    }

    [Fact]
    public void CreateQuery_generic_creates_Db2Queryable_for_element_type()
    {
        var provider = new Db2ContextQueryProvider(new NoOpContext());

        var expression = Expression.Constant(Enumerable.Empty<int>().AsQueryable());

        var query = provider.CreateQuery<int>(expression);

        query.ShouldBeOfType<Db2Queryable<int>>();
    }

    [Fact]
    public void Execute_non_generic_throws_not_supported()
    {
        var provider = new Db2ContextQueryProvider(new NoOpContext());

        var expression = Expression.Constant(1);

        Should.Throw<NotSupportedException>(() => provider.Execute(expression));
    }

    [Fact]
    public void Execute_generic_throws_when_root_table_cannot_be_located()
    {
        var provider = new Db2ContextQueryProvider(new NoOpContext());

        var expression = Expression.Constant(1);

        Should.Throw<NotSupportedException>(() => provider.Execute<int>(expression));
    }
}
