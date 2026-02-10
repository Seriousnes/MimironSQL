using System.Linq.Expressions;

using Microsoft.EntityFrameworkCore.Query;

using MimironSQL.EntityFrameworkCore.Query;
using MimironSQL.EntityFrameworkCore.Query.Internal;

using NSubstitute;

using Shouldly;

namespace MimironSQL.EntityFrameworkCore.Tests;

public sealed class MimironDb2AsyncQueryAdapterTests
{
    [Fact]
    public async Task ExecuteAsync_supports_Task_and_ValueTask()
    {
        var executor = Substitute.For<IMimironDb2QueryExecutor>();
        var query = Expression.Constant(123);

        executor.Execute<int>(query).Returns(7);

        (await MimironDb2AsyncQueryAdapter.ExecuteAsync<Task<int>>(executor, query, CancellationToken.None))
            .ShouldBe(7);

        (await MimironDb2AsyncQueryAdapter.ExecuteAsync<ValueTask<int>>(executor, query, CancellationToken.None))
            .ShouldBe(7);

        (await MimironDb2AsyncQueryAdapter.ExecuteAsync<Task<int>>(executor, query))
            .ShouldBe(7);

        executor.Received(3).Execute<int>(query);
    }

    [Fact]
    public async Task ExecuteAsync_supports_IAsyncEnumerable_and_cancellation()
    {
        var executor = Substitute.For<IMimironDb2QueryExecutor>();
        var query = Expression.Constant(123);

        executor.Execute<IEnumerable<int>>(query).Returns([1, 2]);

        // Case A: no stored cancellation token.
        var asyncEnumerableA = MimironDb2AsyncQueryAdapter.ExecuteAsync<IAsyncEnumerable<int>>(executor, query, CancellationToken.None);
        var enumeratorA = asyncEnumerableA.GetAsyncEnumerator();
        (await enumeratorA.MoveNextAsync()).ShouldBeTrue();
        enumeratorA.Current.ShouldBe(1);
        await enumeratorA.DisposeAsync();

        // Case B: stored token is cancellable, caller token is not.
        using var storedCts = new CancellationTokenSource();
        var asyncEnumerableB = MimironDb2AsyncQueryAdapter.ExecuteAsync<IAsyncEnumerable<int>>(executor, query, storedCts.Token);
        var resultsB = new List<int>();
        await foreach (var value in asyncEnumerableB)
            resultsB.Add(value);

        resultsB.ShouldBe([1, 2]);

        // Case C: both tokens cancellable => linked CTS + cancellation is observed.
        using var storedCts2 = new CancellationTokenSource();
        using var callerCts = new CancellationTokenSource();

        var asyncEnumerableC = MimironDb2AsyncQueryAdapter.ExecuteAsync<IAsyncEnumerable<int>>(executor, query, storedCts2.Token);
        var enumeratorC = asyncEnumerableC.GetAsyncEnumerator(callerCts.Token);

        callerCts.Cancel();

        try
        {
            await Should.ThrowAsync<OperationCanceledException>(async () => await enumeratorC.MoveNextAsync());
        }
        finally
        {
            await enumeratorC.DisposeAsync();
        }

        executor.Received(3).Execute<IEnumerable<int>>(query);
    }

    [Fact]
    public void ExecuteAsync_throws_for_unsupported_result_types()
    {
        var executor = Substitute.For<IMimironDb2QueryExecutor>();
        var query = Expression.Constant(123);

        Should.Throw<NotSupportedException>(() =>
            MimironDb2AsyncQueryAdapter.ExecuteAsync<int>(executor, query, CancellationToken.None));
    }

    [Fact]
    public async Task PrecompileQuery_supports_sync_and_async()
    {
        var executor = Substitute.For<IMimironDb2QueryExecutor>();
        var query = Expression.Constant(123);

        executor.Execute<int>(query).Returns(7);

        var sync = MimironDb2AsyncQueryAdapter.PrecompileQuery<int>(executor, query, async: false).Compile();
        sync(null!).ShouldBe(7);

        var async = MimironDb2AsyncQueryAdapter.PrecompileQuery<Task<int>>(executor, query, async: true).Compile();
        (await async(null!)).ShouldBe(7);

        executor.Received(2).Execute<int>(query);
    }
}
