using MimironSQL.Db2;
using MimironSQL.Dbd;
using NSubstitute;

namespace MimironSQL.EntityFrameworkCore.Tests;

internal static class TestHelpers
{
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
