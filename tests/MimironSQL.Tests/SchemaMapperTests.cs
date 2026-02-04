using System.Text;

using MimironSQL.Db2;
using MimironSQL.Db2.Schema;
using MimironSQL.Dbd;
using MimironSQL.Formats;
using MimironSQL.Providers;

using NSubstitute;

using Shouldly;

namespace MimironSQL.Tests;

public sealed class SchemaMapperTests
{
    [Fact]
    public void SchemaMapper_GetSchema_NoMatchingLayout_Throws()
    {
        var provider = Substitute.For<IDbdProvider>();
        provider.Open("Foo").Returns(ParseDbd("LAYOUT 11111111\n"));

        var mapper = new SchemaMapper(provider);

        var ex = Should.Throw<InvalidDataException>(() => mapper.GetSchema("Foo", new Db2FileLayout(layoutHash: 0x22222222, physicalFieldsCount: 0)));
        ex.Message.ShouldContain("No matching LAYOUT");
        ex.Message.ShouldContain("22222222");
        ex.Message.ShouldContain("Foo.dbd");
    }

    [Fact]
    public void SchemaMapper_GetSchema_NoMatchingBuild_ThrowsAndListsCounts()
    {
        var dbd = """
        COLUMNS
        int ID
        int A
        int B

        LAYOUT 12345678
        BUILD 1
        ID
        A

        BUILD 2
        ID
        $noninline$ B
        """;

        var provider = Substitute.For<IDbdProvider>();
        provider.Open("Foo").Returns(ParseDbd(dbd));

        var mapper = new SchemaMapper(provider);

        var ex = Should.Throw<InvalidDataException>(() => mapper.GetSchema("Foo", new Db2FileLayout(layoutHash: 0x12345678, physicalFieldsCount: 3)));
        ex.Message.ShouldContain("matches physical column count 3");
        ex.Message.ShouldContain("Available:");
        ex.Message.ShouldContain("2");
        ex.Message.ShouldContain("1");
    }

    [Fact]
    public void SchemaMapper_GetSchema_MapsNonInlineEntriesToVirtualIndexes()
    {
        var dbd = """
        COLUMNS
        int ID
        int ParentID
        int Foo

        LAYOUT CAFEBABE
        BUILD 1
        $noninline,id$ ID
        $noninline,relation$ ParentID
        Foo
        """;

        var provider = Substitute.For<IDbdProvider>();
        provider.Open("Foo").Returns(ParseDbd(dbd));

        var mapper = new SchemaMapper(provider);

        var schema = mapper.GetSchema("Foo", new Db2FileLayout(layoutHash: 0xCAFEBABE, physicalFieldsCount: 1));
        schema.TableName.ShouldBe("Foo");
        schema.LayoutHash.ShouldBe(0xCAFEBABEu);
        schema.PhysicalColumnCount.ShouldBe(1);
        schema.Fields.Count.ShouldBe(3);

        schema.Fields[0].Name.ShouldBe("ID");
        schema.Fields[0].IsVirtual.ShouldBeTrue();
        schema.Fields[0].IsId.ShouldBeTrue();
        schema.Fields[0].ColumnStartIndex.ShouldBe(Db2VirtualFieldIndex.Id);

        schema.Fields[1].Name.ShouldBe("ParentID");
        schema.Fields[1].IsVirtual.ShouldBeTrue();
        schema.Fields[1].IsRelation.ShouldBeTrue();
        schema.Fields[1].ColumnStartIndex.ShouldBe(Db2VirtualFieldIndex.ParentRelation);

        schema.Fields[2].Name.ShouldBe("Foo");
        schema.Fields[2].IsVirtual.ShouldBeFalse();
        schema.Fields[2].ColumnStartIndex.ShouldBe(0);
    }

    private static IDbdFile ParseDbd(string text)
    {
        using var stream = new MemoryStream(Encoding.UTF8.GetBytes(text));
        return DbdFile.Parse(stream);
    }
}
