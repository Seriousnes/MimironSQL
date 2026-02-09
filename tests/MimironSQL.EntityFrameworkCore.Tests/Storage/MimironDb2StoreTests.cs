using MimironSQL.EntityFrameworkCore.Storage;
using MimironSQL.Formats;
using MimironSQL.Providers;

using NSubstitute;

using Shouldly;

namespace MimironSQL.EntityFrameworkCore.Tests.Storage;

public class MimironDb2StoreTests
{
    [Fact]
    public void Constructor_WithNullDb2StreamProvider_ShouldThrow()
    {
        var dbdProvider = Substitute.For<IDbdProvider>();
        var format = Substitute.For<IDb2Format>();

        Should.Throw<ArgumentNullException>(() => new MimironDb2Store(null!, dbdProvider, format));
    }

    [Fact]
    public void Constructor_WithNullDbdProvider_ShouldThrow()
    {
        var db2StreamProvider = Substitute.For<IDb2StreamProvider>();
        var format = Substitute.For<IDb2Format>();

        Should.Throw<ArgumentNullException>(() => new MimironDb2Store(db2StreamProvider, null!, format));
    }

    [Fact]
    public void Constructor_WithNullFormat_ShouldThrow()
    {
        var db2StreamProvider = Substitute.For<IDb2StreamProvider>();
        var dbdProvider = Substitute.For<IDbdProvider>();

        Should.Throw<ArgumentNullException>(() => new MimironDb2Store(db2StreamProvider, dbdProvider, null!));
    }

    [Fact]
    public void Constructor_WithValidArguments_ShouldSucceed()
    {
        var db2StreamProvider = Substitute.For<IDb2StreamProvider>();
        var dbdProvider = Substitute.For<IDbdProvider>();
        var format = Substitute.For<IDb2Format>();

        var store = new MimironDb2Store(db2StreamProvider, dbdProvider, format);

        store.ShouldNotBeNull();
    }

    [Fact]
    public void OpenTableWithSchema_WithNullTableName_ShouldThrow()
    {
        var db2StreamProvider = Substitute.For<IDb2StreamProvider>();
        var dbdProvider = Substitute.For<IDbdProvider>();
        var format = Substitute.For<IDb2Format>();
        var store = new MimironDb2Store(db2StreamProvider, dbdProvider, format);

        Should.Throw<ArgumentException>(() => store.OpenTableWithSchema(null!));
    }

    [Fact]
    public void OpenTableWithSchema_WithEmptyTableName_ShouldThrow()
    {
        var db2StreamProvider = Substitute.For<IDb2StreamProvider>();
        var dbdProvider = Substitute.For<IDbdProvider>();
        var format = Substitute.For<IDb2Format>();
        var store = new MimironDb2Store(db2StreamProvider, dbdProvider, format);

        Should.Throw<ArgumentException>(() => store.OpenTableWithSchema(""));
    }

    [Fact]
    public void OpenTableWithSchema_WithWhitespaceTableName_ShouldThrow()
    {
        var db2StreamProvider = Substitute.For<IDb2StreamProvider>();
        var dbdProvider = Substitute.For<IDbdProvider>();
        var format = Substitute.For<IDb2Format>();
        var store = new MimironDb2Store(db2StreamProvider, dbdProvider, format);

        Should.Throw<ArgumentException>(() => store.OpenTableWithSchema("   "));
    }

    [Fact]
    public void OpenTable_ShouldReturnFile()
    {
        var tableName = "TestTable";
        var stream = new MemoryStream();
        var file = Substitute.For<IDb2File>();
        var dbdFile = TestHelpers.CreateMockDbdFile();
        var layout = new Db2FileLayout(0x12345678, 5);

        var db2StreamProvider = Substitute.For<IDb2StreamProvider>();
        db2StreamProvider.OpenDb2Stream(tableName).Returns(stream);

        var dbdProvider = Substitute.For<IDbdProvider>();
        dbdProvider.Open(tableName).Returns(dbdFile);

        var format = Substitute.For<IDb2Format>();
        format.OpenFile(stream).Returns(file);
        format.GetLayout(file).Returns(layout);

        var store = new MimironDb2Store(db2StreamProvider, dbdProvider, format);

        var result = store.OpenTable(tableName);

        result.ShouldBe(file);
    }

    [Fact]
    public void OpenTable_Generic_ShouldReturnTypedFile()
    {
        var tableName = "TestTable";
        var stream = new MemoryStream();
        var file = Substitute.For<IDb2File<RowHandle>>();
        var dbdFile = TestHelpers.CreateMockDbdFile();
        var layout = new Db2FileLayout(0x12345678, 5);

        var db2StreamProvider = Substitute.For<IDb2StreamProvider>();
        db2StreamProvider.OpenDb2Stream(tableName).Returns(stream);

        var dbdProvider = Substitute.For<IDbdProvider>();
        dbdProvider.Open(tableName).Returns(dbdFile);

        var format = Substitute.For<IDb2Format>();
        format.OpenFile(stream).Returns(file);
        format.GetLayout(file).Returns(layout);
        file.RowType.Returns(typeof(RowHandle));

        var store = new MimironDb2Store(db2StreamProvider, dbdProvider, format);

        var result = store.OpenTable<RowHandle>(tableName);

        result.ShouldBe(file);
    }

    [Fact]
    public void GetSchema_ShouldReturnSchema()
    {
        var tableName = "TestTable";
        var stream = new MemoryStream();
        var file = Substitute.For<IDb2File>();
        var dbdFile = TestHelpers.CreateMockDbdFile();
        var layout = new Db2FileLayout(0x12345678, 5);

        var db2StreamProvider = Substitute.For<IDb2StreamProvider>();
        db2StreamProvider.OpenDb2Stream(tableName).Returns(stream);

        var dbdProvider = Substitute.For<IDbdProvider>();
        dbdProvider.Open(tableName).Returns(dbdFile);

        var format = Substitute.For<IDb2Format>();
        format.OpenFile(stream).Returns(file);
        format.GetLayout(file).Returns(layout);

        var store = new MimironDb2Store(db2StreamProvider, dbdProvider, format);

        var result = store.GetSchema(tableName);

        result.ShouldNotBeNull();
        result.TableName.ShouldBe(tableName);
        result.LayoutHash.ShouldBe(layout.LayoutHash);
    }

    [Fact]
    public void OpenTableWithSchema_ShouldReturnFileAndSchema()
    {
        var tableName = "TestTable";
        var stream = new MemoryStream();
        var file = Substitute.For<IDb2File>();
        var dbdFile = TestHelpers.CreateMockDbdFile();
        var layout = new Db2FileLayout(0x12345678, 5);

        var db2StreamProvider = Substitute.For<IDb2StreamProvider>();
        db2StreamProvider.OpenDb2Stream(tableName).Returns(stream);

        var dbdProvider = Substitute.For<IDbdProvider>();
        dbdProvider.Open(tableName).Returns(dbdFile);

        var format = Substitute.For<IDb2Format>();
        format.OpenFile(stream).Returns(file);
        format.GetLayout(file).Returns(layout);

        var store = new MimironDb2Store(db2StreamProvider, dbdProvider, format);

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
        var dbdFile = TestHelpers.CreateMockDbdFile();
        var layout = new Db2FileLayout(0x12345678, 5);

        var db2StreamProvider = Substitute.For<IDb2StreamProvider>();
        db2StreamProvider.OpenDb2Stream(tableName).Returns(stream);

        var dbdProvider = Substitute.For<IDbdProvider>();
        dbdProvider.Open(tableName).Returns(dbdFile);

        var format = Substitute.For<IDb2Format>();
        format.OpenFile(stream).Returns(file);
        format.GetLayout(file).Returns(layout);
        file.RowType.Returns(typeof(RowHandle));

        var store = new MimironDb2Store(db2StreamProvider, dbdProvider, format);

        var (resultFile, resultSchema) = store.OpenTableWithSchema<RowHandle>(tableName);

        resultFile.ShouldBe(file);
        resultSchema.ShouldNotBeNull();
        resultSchema.TableName.ShouldBe(tableName);
    }

    [Fact]
    public void OpenTableWithSchema_CalledMultipleTimes_ShouldCacheResults()
    {
        var tableName = "TestTable";
        var stream = new MemoryStream();
        var file = Substitute.For<IDb2File>();
        var dbdFile = TestHelpers.CreateMockDbdFile();
        var layout = new Db2FileLayout(0x12345678, 5);

        var db2StreamProvider = Substitute.For<IDb2StreamProvider>();
        db2StreamProvider.OpenDb2Stream(tableName).Returns(stream);

        var dbdProvider = Substitute.For<IDbdProvider>();
        dbdProvider.Open(tableName).Returns(dbdFile);

        var format = Substitute.For<IDb2Format>();
        format.OpenFile(stream).Returns(file);
        format.GetLayout(file).Returns(layout);

        var store = new MimironDb2Store(db2StreamProvider, dbdProvider, format);

        var (file1, schema1) = store.OpenTableWithSchema(tableName);
        var (file2, schema2) = store.OpenTableWithSchema(tableName);

        file1.ShouldBe(file2);
        schema1.ShouldBe(schema2);

        db2StreamProvider.Received(1).OpenDb2Stream(tableName);
        dbdProvider.Received(1).Open(tableName);
        format.Received(1).OpenFile(stream);
    }

    [Fact]
    public void OpenTableWithSchema_CaseInsensitive_ShouldUseSameCache()
    {
        var tableName = "TestTable";
        var stream = new MemoryStream();
        var file = Substitute.For<IDb2File>();
        var dbdFile = TestHelpers.CreateMockDbdFile();
        var layout = new Db2FileLayout(0x12345678, 5);

        var db2StreamProvider = Substitute.For<IDb2StreamProvider>();
        db2StreamProvider.OpenDb2Stream(Arg.Any<string>()).Returns(stream);

        var dbdProvider = Substitute.For<IDbdProvider>();
        dbdProvider.Open(Arg.Any<string>()).Returns(dbdFile);

        var format = Substitute.For<IDb2Format>();
        format.OpenFile(stream).Returns(file);
        format.GetLayout(file).Returns(layout);

        var store = new MimironDb2Store(db2StreamProvider, dbdProvider, format);

        var (file1, schema1) = store.OpenTableWithSchema(tableName);
        var (file2, schema2) = store.OpenTableWithSchema("TESTTABLE");
        var (file3, schema3) = store.OpenTableWithSchema("testtable");

        file1.ShouldBe(file2);
        file1.ShouldBe(file3);
        schema1.ShouldBe(schema2);
        schema1.ShouldBe(schema3);

        db2StreamProvider.Received(1).OpenDb2Stream(Arg.Any<string>());
    }

    [Fact]
    public void OpenTable_Generic_WithMismatchedRowType_ShouldThrow()
    {
        var tableName = "TestTable";
        var stream = new MemoryStream();
        var file = Substitute.For<IDb2File<RowHandle>>();
        var dbdFile = TestHelpers.CreateMockDbdFile();
        var layout = new Db2FileLayout(0x12345678, 5);

        var db2StreamProvider = Substitute.For<IDb2StreamProvider>();
        db2StreamProvider.OpenDb2Stream(tableName).Returns(stream);

        var dbdProvider = Substitute.For<IDbdProvider>();
        dbdProvider.Open(tableName).Returns(dbdFile);

        var format = Substitute.For<IDb2Format>();
        format.OpenFile(stream).Returns(file);
        format.GetLayout(file).Returns(layout);
        file.RowType.Returns(typeof(RowHandle));

        var store = new MimironDb2Store(db2StreamProvider, dbdProvider, format);

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
        var dbdFile = TestHelpers.CreateMockDbdFile();
        var layout = new Db2FileLayout(0x12345678, 5);

        var db2StreamProvider = Substitute.For<IDb2StreamProvider>();
        db2StreamProvider.OpenDb2Stream(tableName).Returns(stream);

        var dbdProvider = Substitute.For<IDbdProvider>();
        dbdProvider.Open(tableName).Returns(dbdFile);

        var format = Substitute.For<IDb2Format>();
        format.OpenFile(stream).Returns(file);
        format.GetLayout(file).Returns(layout);
        file.RowType.Returns(typeof(RowHandle));

        var store = new MimironDb2Store(db2StreamProvider, dbdProvider, format);

        var exception = Should.Throw<InvalidOperationException>(() => store.OpenTableWithSchema<TestRow>(tableName));
        exception.Message.ShouldContain("TestRow");
        exception.Message.ShouldContain("RowHandle");
    }

    public struct TestRow
    {
        public int Id { get; set; }
        public int Value { get; set; }
    }
}
