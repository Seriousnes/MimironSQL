using System.Text;

using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;

using MimironSQL.Dbd;
using MimironSQL.EntityFrameworkCore.Infrastructure;
using MimironSQL.EntityFrameworkCore.Storage;
using MimironSQL.Formats;
using MimironSQL.Providers;

using NSubstitute;

using Shouldly;

namespace MimironSQL.EntityFrameworkCore.Tests.Storage;

public class MimironDb2StoreTests
{
    private const string WowVersion = TestHelpers.WowVersion;

    [Fact]
    public void Constructor_WithNullDb2StreamProvider_ShouldThrow()
    {
        var dbdProvider = Substitute.For<IDbdProvider>();
        var format = Substitute.For<IDb2Format>();

        Should.Throw<ArgumentNullException>(() => new MimironDb2Store(null!, dbdProvider, format, CreateOptions()));
    }

    [Fact]
    public void Constructor_WithNullDbdProvider_ShouldThrow()
    {
        var db2StreamProvider = Substitute.For<IDb2StreamProvider>();
        var format = Substitute.For<IDb2Format>();

        Should.Throw<ArgumentNullException>(() => new MimironDb2Store(db2StreamProvider, null!, format, CreateOptions()));
    }

    [Fact]
    public void Constructor_WithNullFormat_ShouldThrow()
    {
        var db2StreamProvider = Substitute.For<IDb2StreamProvider>();
        var dbdProvider = Substitute.For<IDbdProvider>();

        Should.Throw<ArgumentNullException>(() => new MimironDb2Store(db2StreamProvider, dbdProvider, null!, CreateOptions()));
    }

    [Fact]
    public void Constructor_WithNullContextOptions_ShouldThrow()
    {
        var db2StreamProvider = Substitute.For<IDb2StreamProvider>();
        var dbdProvider = Substitute.For<IDbdProvider>();
        var format = Substitute.For<IDb2Format>();

        Should.Throw<ArgumentNullException>(() => new MimironDb2Store(db2StreamProvider, dbdProvider, format, null!));
    }

    [Fact]
    public void Constructor_WithValidArguments_ShouldSucceed()
    {
        var db2StreamProvider = Substitute.For<IDb2StreamProvider>();
        var dbdProvider = Substitute.For<IDbdProvider>();
        var format = Substitute.For<IDb2Format>();

        var store = new MimironDb2Store(db2StreamProvider, dbdProvider, format, CreateOptions());

        store.ShouldNotBeNull();
    }

    [Fact]
    public void OpenTableWithSchema_WithNullTableName_ShouldThrow()
    {
        var db2StreamProvider = Substitute.For<IDb2StreamProvider>();
        var dbdProvider = Substitute.For<IDbdProvider>();
        var format = Substitute.For<IDb2Format>();
        var store = new MimironDb2Store(db2StreamProvider, dbdProvider, format, CreateOptions());

        Should.Throw<ArgumentException>(() => store.OpenTableWithSchema(null!));
    }

    [Fact]
    public void OpenTableWithSchema_WithEmptyTableName_ShouldThrow()
    {
        var db2StreamProvider = Substitute.For<IDb2StreamProvider>();
        var dbdProvider = Substitute.For<IDbdProvider>();
        var format = Substitute.For<IDb2Format>();
        var store = new MimironDb2Store(db2StreamProvider, dbdProvider, format, CreateOptions());

        Should.Throw<ArgumentException>(() => store.OpenTableWithSchema(""));
    }

    [Fact]
    public void OpenTableWithSchema_WithWhitespaceTableName_ShouldThrow()
    {
        var db2StreamProvider = Substitute.For<IDb2StreamProvider>();
        var dbdProvider = Substitute.For<IDbdProvider>();
        var format = Substitute.For<IDb2Format>();
        var store = new MimironDb2Store(db2StreamProvider, dbdProvider, format, CreateOptions());

        Should.Throw<ArgumentException>(() => store.OpenTableWithSchema("   "));
    }

    [Fact]
    public void OpenTable_ShouldReturnFile()
    {
        var tableName = "TestTable";
        var stream = new MemoryStream();
        var file = Substitute.For<IDb2File>();
        var layout = new Db2FileLayout(0x12345678, 5);

        var db2StreamProvider = Substitute.For<IDb2StreamProvider>();
        db2StreamProvider.OpenDb2Stream(tableName).Returns(stream);

        var dbdProvider = Substitute.For<IDbdProvider>();
        dbdProvider.Open(tableName).Returns(ParseDbd(TestDbd));

        var format = Substitute.For<IDb2Format>();
        format.OpenFile(stream).Returns(file);
        format.GetLayout(file).Returns(layout);

        var store = new MimironDb2Store(db2StreamProvider, dbdProvider, format, CreateOptions());

        var result = store.OpenTable(tableName);

        result.ShouldBe(file);
    }

    [Fact]
    public void OpenTable_Generic_ShouldReturnTypedFile()
    {
        var tableName = "TestTable";
        var stream = new MemoryStream();
        var file = Substitute.For<IDb2File<RowHandle>>();
        var layout = new Db2FileLayout(0x12345678, 5);

        var db2StreamProvider = Substitute.For<IDb2StreamProvider>();
        db2StreamProvider.OpenDb2Stream(tableName).Returns(stream);

        var dbdProvider = Substitute.For<IDbdProvider>();
        dbdProvider.Open(tableName).Returns(ParseDbd(TestDbd));

        var format = Substitute.For<IDb2Format>();
        format.OpenFile(stream).Returns(file);
        format.GetLayout(file).Returns(layout);
        file.RowType.Returns(typeof(RowHandle));

        var store = new MimironDb2Store(db2StreamProvider, dbdProvider, format, CreateOptions());

        var result = store.OpenTable<RowHandle>(tableName);

        result.ShouldBe(file);
    }

    [Fact]
    public void GetSchema_ShouldReturnSchema()
    {
        var tableName = "TestTable";
        var db2StreamProvider = Substitute.For<IDb2StreamProvider>();
        var dbdProvider = Substitute.For<IDbdProvider>();
        dbdProvider.Open(tableName).Returns(ParseDbd(TestDbd));

        var format = Substitute.For<IDb2Format>();

        var store = new MimironDb2Store(db2StreamProvider, dbdProvider, format, CreateOptions());

        var result = store.GetSchema(tableName);

        result.ShouldNotBeNull();
        result.TableName.ShouldBe(tableName);
        result.AllowsAnyLayoutHash.ShouldBeTrue();
    }

    [Fact]
    public void OpenTableWithSchema_ShouldReturnFileAndSchema()
    {
        var tableName = "TestTable";
        var stream = new MemoryStream();
        var file = Substitute.For<IDb2File>();
        var layout = new Db2FileLayout(0x12345678, 5);

        var db2StreamProvider = Substitute.For<IDb2StreamProvider>();
        db2StreamProvider.OpenDb2Stream(tableName).Returns(stream);

        var dbdProvider = Substitute.For<IDbdProvider>();
        dbdProvider.Open(tableName).Returns(ParseDbd(TestDbd));

        var format = Substitute.For<IDb2Format>();
        format.OpenFile(stream).Returns(file);
        format.GetLayout(file).Returns(layout);

        var store = new MimironDb2Store(db2StreamProvider, dbdProvider, format, CreateOptions());

        var (resultFile, resultSchema) = store.OpenTableWithSchema(tableName);

        resultFile.ShouldBe(file);
        resultSchema.ShouldNotBeNull();
        resultSchema.TableName.ShouldBe(tableName);
    }

    [Fact]
    public void OpenTableWithSchema_Generic_ShouldReturnTypedFileAndSchema()
    {
        var tableName = "TestTable";
        var stream = new MemoryStream();
        var file = Substitute.For<IDb2File<RowHandle>>();
        var layout = new Db2FileLayout(0x12345678, 5);

        var db2StreamProvider = Substitute.For<IDb2StreamProvider>();
        db2StreamProvider.OpenDb2Stream(tableName).Returns(stream);

        var dbdProvider = Substitute.For<IDbdProvider>();
        dbdProvider.Open(tableName).Returns(ParseDbd(TestDbd));

        var format = Substitute.For<IDb2Format>();
        format.OpenFile(stream).Returns(file);
        format.GetLayout(file).Returns(layout);
        file.RowType.Returns(typeof(RowHandle));

        var store = new MimironDb2Store(db2StreamProvider, dbdProvider, format, CreateOptions());

        var (resultFile, resultSchema) = store.OpenTableWithSchema<RowHandle>(tableName);

        resultFile.ShouldBe(file);
        resultSchema.ShouldNotBeNull();
        resultSchema.TableName.ShouldBe(tableName);
    }

    [Fact]
    public void OpenTableWithSchema_CalledMultipleTimes_ShouldCacheResults()
    {
        var tableName = "TestTable";
        var layout = new Db2FileLayout(0x12345678, 5);

        var db2StreamProvider = Substitute.For<IDb2StreamProvider>();
        db2StreamProvider.OpenDb2Stream(tableName).Returns(_ => new MemoryStream());

        var dbdProvider = Substitute.For<IDbdProvider>();
        dbdProvider.Open(tableName).Returns(ParseDbd(TestDbd));

        var format = Substitute.For<IDb2Format>();
        format.OpenFile(Arg.Any<Stream>()).Returns(_ => Substitute.For<IDb2File>());
        format.GetLayout(Arg.Any<IDb2File>()).Returns(layout);

        var store = new MimironDb2Store(db2StreamProvider, dbdProvider, format, CreateOptions());

        var (file1, schema1) = store.OpenTableWithSchema(tableName);
        var (file2, schema2) = store.OpenTableWithSchema(tableName);

        file1.ShouldNotBe(file2);
        schema1.ShouldBe(schema2);

        db2StreamProvider.Received(2).OpenDb2Stream(tableName);
        dbdProvider.Received(1).Open(tableName);
        format.Received(2).OpenFile(Arg.Any<Stream>());
    }

    [Fact]
    public void OpenTableWithSchema_CaseInsensitive_ShouldUseSameCache()
    {
        var tableName = "TestTable";
        var layout = new Db2FileLayout(0x12345678, 5);

        var db2StreamProvider = Substitute.For<IDb2StreamProvider>();
        db2StreamProvider.OpenDb2Stream(Arg.Any<string>()).Returns(_ => new MemoryStream());

        var dbdProvider = Substitute.For<IDbdProvider>();
        dbdProvider.Open(Arg.Any<string>()).Returns(_ => ParseDbd(TestDbd));

        var format = Substitute.For<IDb2Format>();
        format.OpenFile(Arg.Any<Stream>()).Returns(_ => Substitute.For<IDb2File>());
        format.GetLayout(Arg.Any<IDb2File>()).Returns(layout);

        var store = new MimironDb2Store(db2StreamProvider, dbdProvider, format, CreateOptions());

        var (file1, schema1) = store.OpenTableWithSchema(tableName);
        var (file2, schema2) = store.OpenTableWithSchema("TESTTABLE");
        var (file3, schema3) = store.OpenTableWithSchema("testtable");

        file1.ShouldNotBe(file2);
        file1.ShouldNotBe(file3);
        schema1.ShouldBe(schema2);
        schema1.ShouldBe(schema3);

        db2StreamProvider.Received(3).OpenDb2Stream(Arg.Any<string>());
    }

    [Fact]
    public void OpenTable_Generic_WithMismatchedRowType_ShouldThrow()
    {
        var tableName = "TestTable";
        var stream = new MemoryStream();
        var file = Substitute.For<IDb2File<RowHandle>>();
        var layout = new Db2FileLayout(0x12345678, 5);

        var db2StreamProvider = Substitute.For<IDb2StreamProvider>();
        db2StreamProvider.OpenDb2Stream(tableName).Returns(stream);

        var dbdProvider = Substitute.For<IDbdProvider>();
        dbdProvider.Open(tableName).Returns(ParseDbd(TestDbd));

        var format = Substitute.For<IDb2Format>();
        format.OpenFile(stream).Returns(file);
        format.GetLayout(file).Returns(layout);
        file.RowType.Returns(typeof(RowHandle));

        var store = new MimironDb2Store(db2StreamProvider, dbdProvider, format, CreateOptions());

        var exception = Should.Throw<InvalidOperationException>(() => store.OpenTable<TestRow>(tableName));
        exception.Message.ShouldContain("TestRow");
        exception.Message.ShouldContain("RowHandle");
    }

    [Fact]
    public void OpenTableWithSchema_Generic_WithMismatchedRowType_ShouldThrow()
    {
        var tableName = "TestTable";
        var stream = new MemoryStream();
        var file = Substitute.For<IDb2File<RowHandle>>();
        var layout = new Db2FileLayout(0x12345678, 5);

        var db2StreamProvider = Substitute.For<IDb2StreamProvider>();
        db2StreamProvider.OpenDb2Stream(tableName).Returns(stream);

        var dbdProvider = Substitute.For<IDbdProvider>();
        dbdProvider.Open(tableName).Returns(ParseDbd(TestDbd));

        var format = Substitute.For<IDb2Format>();
        format.OpenFile(stream).Returns(file);
        format.GetLayout(file).Returns(layout);
        file.RowType.Returns(typeof(RowHandle));

        var store = new MimironDb2Store(db2StreamProvider, dbdProvider, format, CreateOptions());

        var exception = Should.Throw<InvalidOperationException>(() => store.OpenTableWithSchema<TestRow>(tableName));
        exception.Message.ShouldContain("TestRow");
        exception.Message.ShouldContain("RowHandle");
    }

    public struct TestRow
    {
        public int Id { get; set; }
        public int Value { get; set; }
    }

    private const string TestDbd = """
    COLUMNS
    int ID
    int Field1
    string Field2
    float Field3
    int Field4
    int Field5

    BUILD 12.0.1.65867
    $noninline,id$ ID
    Field1
    Field2
    Field3
    Field4
    Field5
    """;

    private static IDbdFile ParseDbd(string text)
    {
        using var stream = new MemoryStream(Encoding.UTF8.GetBytes(text));
        return DbdFile.Parse(stream);
    }

    private static IDbContextOptions CreateOptions()
    {
        var builder = new DbContextOptionsBuilder();

        var extension = new MimironDb2OptionsExtension().WithWowVersion(WowVersion);
        ((IDbContextOptionsBuilderInfrastructure)builder).AddOrUpdateExtension(extension);

        return builder.Options;
    }
}
