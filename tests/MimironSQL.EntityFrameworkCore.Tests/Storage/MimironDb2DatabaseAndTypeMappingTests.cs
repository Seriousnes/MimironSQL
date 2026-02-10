using System.Linq.Expressions;

using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Storage.ValueConversion;

using MimironSQL.EntityFrameworkCore.Query;
using MimironSQL.EntityFrameworkCore.Storage;

using NSubstitute;

using Shouldly;

namespace MimironSQL.EntityFrameworkCore.Tests;

public sealed class MimironDb2DatabaseAndTypeMappingTests
{
    [Fact]
    public void Database_is_read_only()
    {
        var queryExecutor = Substitute.For<IMimironDb2QueryExecutor>();
        var db = new MimironDb2Database(queryExecutor);

        Should.Throw<NotSupportedException>(() => db.SaveChanges([]))
            .Message.ShouldContain("read-only");

        Should.Throw<NotSupportedException>(() => db.SaveChangesAsync([], CancellationToken.None))
            .Message.ShouldContain("read-only");
    }

    [Fact]
    public async Task CompileQuery_executes_via_executor_for_sync_and_async_over_sync()
    {
        var queryExecutor = Substitute.For<IMimironDb2QueryExecutor>();
        var db = new MimironDb2Database(queryExecutor);

        var query = Expression.Constant(123);

        queryExecutor.Execute<int>(query).Returns(123);

        var syncFunc = db.CompileQuery<int>(query, async: false);
        syncFunc(null!).ShouldBe(123);

        var asyncFunc = db.CompileQuery<Task<int>>(query, async: true);
        (await asyncFunc(null!)).ShouldBe(123);
    }

    [Fact]
    public async Task CompileQueryExpression_can_be_compiled_for_sync_and_async_over_sync()
    {
        var queryExecutor = Substitute.For<IMimironDb2QueryExecutor>();
        var db = new MimironDb2Database(queryExecutor);

        var query = Expression.Constant(123);
        queryExecutor.Execute<int>(query).Returns(5);

        var sync = db.CompileQueryExpression<int>(query, async: false).Compile();
        sync(null!).ShouldBe(5);

        var async = db.CompileQueryExpression<Task<int>>(query, async: true).Compile();
        (await async(null!)).ShouldBe(5);
    }

    [Fact]
    public void TypeMapping_can_be_cloned_with_composed_converter()
    {
        var mapping = new MimironDb2TypeMapping(typeof(int));
        var converter = new ValueConverter<int, int>(v => v, v => v);

        var cloned = mapping.WithComposedConverter(converter);

        cloned.ShouldNotBeSameAs(mapping);
        cloned.ClrType.ShouldBe(typeof(int));
    }
}
