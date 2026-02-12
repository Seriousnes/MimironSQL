using System.Text;

using MimironSQL.Db2;
using MimironSQL.Dbd;
using MimironSQL.EntityFrameworkCore.Db2.Schema;
using MimironSQL.Providers;

using NSubstitute;

using Shouldly;

namespace MimironSQL.EntityFrameworkCore.Tests;

public sealed class SchemaMapperTests
{
    [Fact]
    public void SchemaMapper_GetSchema_NoCompatibleBuild_Throws()
    {
        var provider = Substitute.For<IDbdProvider>();
        provider.Open("Foo").Returns(ParseDbd("""
        COLUMNS
        int ID

        LAYOUT 11111111
        BUILD 1.0.0.10
        ID
        """));

        var mapper = new SchemaMapper(provider, wowVersionRaw: "1.0.0.1");

        var ex = Should.Throw<InvalidDataException>(() => mapper.GetSchema("Foo"));
        ex.Message.ShouldContain("No compatible BUILD blocks");
        ex.Message.ShouldContain("WOW_VERSION=1.0.0.1");
        ex.Message.ShouldContain("Foo.dbd");
    }

    [Fact]
    public void SchemaMapper_GetSchema_LayoutScopedBuild_ProducesAllowedLayoutHashesAllowList()
    {
        var dbd = """
        COLUMNS
        int ID
        int Foo

        LAYOUT CAFEBABE, DEADBEEF
        BUILD 1.0.0.1
        ID
        Foo
        """;

        var provider = Substitute.For<IDbdProvider>();
        provider.Open("Foo").Returns(ParseDbd(dbd));

        var mapper = new SchemaMapper(provider, wowVersionRaw: "1.0.0.1");

        var schema = mapper.GetSchema("Foo");

        schema.AllowsAnyLayoutHash.ShouldBeFalse();
        schema.AllowedLayoutHashes.ShouldNotBeNull();
        schema.AllowedLayoutHashes!.Count.ShouldBe(2);
        schema.IsLayoutHashAllowed(0xCAFEBABEu).ShouldBeTrue();
        schema.IsLayoutHashAllowed(0xDEADBEEFu).ShouldBeTrue();
        schema.IsLayoutHashAllowed(0x12345678u).ShouldBeFalse();
    }

    [Fact]
    public void SchemaMapper_GetSchema_GlobalBuild_ProducesAllowAnyLayoutHash()
    {
        var dbd = """
        COLUMNS
        int ID
        int Foo

        BUILD 1.0.0.1
        ID
        Foo

        LAYOUT CAFEBABE
        BUILD 1.0.0.2
        ID
        Foo
        """;

        var provider = Substitute.For<IDbdProvider>();
        provider.Open("Foo").Returns(ParseDbd(dbd));

        var mapper = new SchemaMapper(provider, wowVersionRaw: "1.0.0.1");

        var schema = mapper.GetSchema("Foo");

        schema.AllowsAnyLayoutHash.ShouldBeTrue();
        schema.AllowedLayoutHashes.ShouldBeNull();
        schema.IsLayoutHashAllowed(0xCAFEBABEu).ShouldBeTrue();
        schema.IsLayoutHashAllowed(0x12345678u).ShouldBeTrue();
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
        BUILD 1.0.0.1
        $noninline,id$ ID
        $noninline,relation$ ParentID
        Foo
        """;

        var provider = Substitute.For<IDbdProvider>();
        provider.Open("Foo").Returns(ParseDbd(dbd));

        var mapper = new SchemaMapper(provider, wowVersionRaw: "1.0.0.1");

        var schema = mapper.GetSchema("Foo");
        schema.TableName.ShouldBe("Foo");
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
