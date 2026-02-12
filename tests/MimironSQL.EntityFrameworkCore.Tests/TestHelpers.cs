using Microsoft.EntityFrameworkCore;

using MimironSQL.Db2;
using MimironSQL.Dbd;
using MimironSQL.EntityFrameworkCore;

using NSubstitute;

namespace MimironSQL.EntityFrameworkCore.Tests;

internal static class TestHelpers
{
    public const string WowVersion = "12.0.1.65867";

    public static DbContextOptionsBuilder UseMimironDb2ForTests(
        this DbContextOptionsBuilder optionsBuilder,
        Action<IMimironDb2DbContextOptionsBuilder> configure)
    {
        ArgumentNullException.ThrowIfNull(optionsBuilder);
        ArgumentNullException.ThrowIfNull(configure);

        return optionsBuilder.UseMimironDb2(o =>
        {
            o.WithWowVersion(WowVersion);
            configure(o);
        });
    }

    public static DbContextOptionsBuilder<TContext> UseMimironDb2ForTests<TContext>(
        this DbContextOptionsBuilder<TContext> optionsBuilder,
        Action<IMimironDb2DbContextOptionsBuilder> configure)
        where TContext : DbContext
    {
        ArgumentNullException.ThrowIfNull(optionsBuilder);
        ArgumentNullException.ThrowIfNull(configure);

        optionsBuilder.UseMimironDb2(o =>
        {
            o.WithWowVersion(WowVersion);
            configure(o);
        });

        return optionsBuilder;
    }

    public static IDbdFile CreateMockDbdFile()
    {
        var dbdFile = Substitute.For<IDbdFile>();
        var layout = Substitute.For<IDbdLayout>();
        var buildBlock = Substitute.For<IDbdBuildBlock>();

        var entries = new List<IDbdLayoutEntry>
        {
            CreateLayoutEntry("Id", Db2ValueType.Int64, isId: true, isNonInline: true),
            CreateLayoutEntry("Field1", Db2ValueType.Int64, elementCount: 1),
            CreateLayoutEntry("Field2", Db2ValueType.String, elementCount: 1),
            CreateLayoutEntry("Field3", Db2ValueType.Single, elementCount: 1),
            CreateLayoutEntry("Field4", Db2ValueType.Int64, elementCount: 1),
            CreateLayoutEntry("Field5", Db2ValueType.Int64, elementCount: 1)
        };

        buildBlock.Entries.Returns(entries);
        layout.TrySelectBuildByPhysicalColumnCount(5, out _, out _)
            .Returns(x =>
            {
                x[1] = buildBlock;
                x[2] = new[] { 5 };
                return true;
            });

        dbdFile.TryGetLayout(Arg.Any<uint>(), out _)
            .Returns(x =>
            {
                x[1] = layout;
                return true;
            });

        return dbdFile;
    }

    private static IDbdLayoutEntry CreateLayoutEntry(
        string name,
        Db2ValueType valueType,
        int elementCount = 0,
        bool isId = false,
        bool isRelation = false,
        bool isNonInline = false,
        string? referencedTableName = null)
    {
        var entry = Substitute.For<IDbdLayoutEntry>();
        entry.Name.Returns(name);
        entry.ValueType.Returns(valueType);
        entry.ElementCount.Returns(elementCount);
        entry.IsId.Returns(isId);
        entry.IsRelation.Returns(isRelation);
        entry.IsNonInline.Returns(isNonInline);
        entry.ReferencedTableName.Returns(referencedTableName);
        entry.IsVerified.Returns(true);
        return entry;
    }
}
