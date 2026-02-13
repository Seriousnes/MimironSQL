using Microsoft.EntityFrameworkCore.Storage.ValueConversion;

using MimironSQL.EntityFrameworkCore.Storage;

using Shouldly;

namespace MimironSQL.EntityFrameworkCore.Tests;

public sealed class MimironDb2DatabaseAndTypeMappingTests
{
    [Fact]
    public void Database_is_read_only()
    {
        var db = new MimironDb2Database();

        Should.Throw<NotSupportedException>(() => db.SaveChanges([]))
            .Message.ShouldContain("read-only");

        Should.Throw<NotSupportedException>(() => db.SaveChangesAsync([], CancellationToken.None))
            .Message.ShouldContain("read-only");
    }

    // CompileQuery and CompileQueryExpression resolve the executor from
    // QueryContext.Context.GetService<IQueryCompiler>() per invocation,
    // which requires a real EF Core service provider.  These paths are exercised
    // by the integration tests in MimironDb2QueryExecutionTests; the normal query
    // path goes through MimironDb2QueryExecutor (IQueryCompiler) instead.

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
