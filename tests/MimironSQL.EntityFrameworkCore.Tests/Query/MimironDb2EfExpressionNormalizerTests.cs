using MimironSQL.EntityFrameworkCore.Query;

using Shouldly;

using System.Linq;
using System.Linq.Expressions;

namespace MimironSQL.EntityFrameworkCore.Tests;

public sealed class MimironDb2EfExpressionNormalizerTests
{
    [Fact]
    public void Normalize_removes_redundant_queryable_asqueryable_when_already_iqueryable()
    {
        var q = new[] { 1, 2, 3 }.AsQueryable();

        Expression<Func<IQueryable<int>>> expr = () => q.AsQueryable();

        var normalized = MimironDb2EfExpressionNormalizer.Normalize(expr.Body);

        normalized.Type.ShouldBe(typeof(IQueryable<int>));
        var eval = Expression.Lambda<Func<IQueryable<int>>>(normalized).Compile().Invoke();
        eval.ShouldBe(q);
    }

    [Fact]
    public void Normalize_preserves_queryable_asqueryable_when_source_is_not_iqueryable()
    {
        Expression<Func<IQueryable<int>>> expr = () => new[] { 1, 2, 3 }.AsQueryable();

        var normalized = MimironDb2EfExpressionNormalizer.Normalize(expr.Body);

        normalized.ShouldBeAssignableTo<MethodCallExpression>().Method.Name.ShouldBe(nameof(Queryable.AsQueryable));
    }

    [Fact]
    public void Normalize_removes_redundant_convert_nodes()
    {
        var redundantConvert = Expression.Convert(Expression.Constant(1), typeof(int));

        var normalized = MimironDb2EfExpressionNormalizer.Normalize(redundantConvert);

        normalized.ShouldBeOfType<ConstantExpression>().Value.ShouldBe(1);
    }
}
