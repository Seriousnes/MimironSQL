using System.Linq.Expressions;

using MimironSQL.EntityFrameworkCore.Db2.Query;

using Shouldly;

namespace MimironSQL.EntityFrameworkCore.Tests;

public sealed class Db2QueryPipelineTests
{
    [Fact]
    public void Parse_recognizes_Skip_operation()
    {
        var query = new[] { 1, 2, 3 }.AsQueryable().Skip(1);

        var pipeline = Db2QueryPipeline.Parse(query.Expression);

        pipeline.Operations.OfType<Db2SkipOperation>().Single().Count.ShouldBe(1);
    }

    [Fact]
    public void Parse_throws_when_Skip_count_is_not_constant()
    {
        var source = Expression.Constant(new[] { 1, 2, 3 }.AsQueryable());
        var count = Expression.Parameter(typeof(int), "count");

        var call = Expression.Call(
            typeof(Queryable),
            nameof(Queryable.Skip),
            [typeof(int)],
            source,
            count);

        Should.Throw<NotSupportedException>(() => Db2QueryPipeline.Parse(call))
            .Message.ShouldContain("constant integer count");
    }

    [Fact]
    public void Parse_records_final_operators_and_predicates()
    {
        var source = new[] { 1, 2, 3 }.AsQueryable();

        Expression<Func<int, bool>> anyPredicate = x => x > 1;
        var anyCall = Expression.Call(
            typeof(Queryable),
            nameof(Queryable.Any),
            [typeof(int)],
            source.Expression,
            Expression.Quote(anyPredicate));

        var anyPipeline = Db2QueryPipeline.Parse(anyCall);
        anyPipeline.FinalOperator.ShouldBe(Db2FinalOperator.Any);
        anyPipeline.Operations.OfType<Db2WhereOperation>().Count().ShouldBe(1);

        Expression<Func<int, bool>> allPredicate = x => x > 0;
        var allCall = Expression.Call(
            typeof(Queryable),
            nameof(Queryable.All),
            [typeof(int)],
            source.Expression,
            Expression.Quote(allPredicate));

        var allPipeline = Db2QueryPipeline.Parse(allCall);
        allPipeline.FinalOperator.ShouldBe(Db2FinalOperator.All);
        allPipeline.Operations.OfType<Db2WhereOperation>().Count().ShouldBe(0);
    }
}
