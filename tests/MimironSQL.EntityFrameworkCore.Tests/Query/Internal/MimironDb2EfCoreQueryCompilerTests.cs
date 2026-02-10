using System.Linq.Expressions;

using MimironSQL.EntityFrameworkCore.Query;
using MimironSQL.EntityFrameworkCore.Query.Internal;

using NSubstitute;

using Shouldly;

namespace MimironSQL.EntityFrameworkCore.Tests;

public sealed class MimironDb2EfCoreQueryCompilerTests
{
    [Fact]
    public void Execute_delegates_to_executor()
    {
        var executor = Substitute.For<IMimironDb2QueryExecutor>();
        var query = Expression.Constant(123);

        executor.Execute<int>(query).Returns(42);

        var compiler = new MimironDb2EfCoreQueryCompiler(executor);

        compiler.Execute<int>(query).ShouldBe(42);
        executor.Received(1).Execute<int>(query);
    }

    [Fact]
    public void CreateCompiledQuery_returns_delegate_that_delegates_to_executor()
    {
        var executor = Substitute.For<IMimironDb2QueryExecutor>();
        var query = Expression.Constant(123);

        executor.Execute<int>(query).Returns(7);

        var compiler = new MimironDb2EfCoreQueryCompiler(executor);
        var compiled = compiler.CreateCompiledQuery<int>(query);

        compiled(null!).ShouldBe(7);
        executor.Received(1).Execute<int>(query);
    }

    [Fact]
    public void Async_and_precompilation_apis_throw_not_supported()
    {
        var executor = Substitute.For<IMimironDb2QueryExecutor>();
        var compiler = new MimironDb2EfCoreQueryCompiler(executor);
        var query = Expression.Constant(123);

        var compiledAsync = compiler.CreateCompiledAsyncQuery<int>(query);
        Should.Throw<NotSupportedException>(() => compiledAsync(null!));

        Should.Throw<NotSupportedException>(() => compiler.ExecuteAsync<int>(query, CancellationToken.None));
        Should.Throw<NotSupportedException>(() => compiler.PrecompileQuery<int>(query, async: false));
    }
}
