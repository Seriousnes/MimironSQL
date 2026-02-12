using System.Text;

using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;

using MimironSQL.Dbd;
using MimironSQL.EntityFrameworkCore.Tests;
using MimironSQL.EntityFrameworkCore.Db2.Model;
using MimironSQL.EntityFrameworkCore.Infrastructure;
using MimironSQL.EntityFrameworkCore.Query;
using MimironSQL.EntityFrameworkCore.Storage;
using MimironSQL.Formats;
using MimironSQL.Providers;

using NSubstitute;

using Shouldly;

namespace MimironSQL.EntityFrameworkCore.Tests.Query;

public sealed class QuerySessionTests
{
    [Fact]
    public void Warm_ResolvesRootExplicitIncludeAndAutoIncludes_ExactlyOncePerTable()
    {
        using var context = CreateContext();

        var model = CreateDb2ModelWithAutoIncludes();
        var (store, db2StreamProvider) = CreateStore();

        var session = new QuerySession<RowHandle>(context, store, model);

        var query = context.Set<Parent>()
            .Include(x => x.Children)
            .Include(x => x.Auto);

        session.Warm(query.Expression, typeof(Parent));

        db2StreamProvider.Received(1).OpenDb2Stream("ParentTable");
        db2StreamProvider.Received(1).OpenDb2Stream("ChildTable");
        db2StreamProvider.Received(1).OpenDb2Stream("AutoTable");
    }

    [Fact]
    public void Warm_WhenIgnoreAutoIncludes_IsSet_DoesNotResolveAutoIncludes()
    {
        using var context = CreateContext();

        var model = CreateDb2ModelWithAutoIncludes();
        var (store, db2StreamProvider) = CreateStore();

        var session = new QuerySession<RowHandle>(context, store, model);

        var query = context.Set<Parent>()
            .Include(x => x.Children)
            .IgnoreAutoIncludes();

        session.Warm(query.Expression, typeof(Parent));

        db2StreamProvider.Received(1).OpenDb2Stream("ParentTable");
        db2StreamProvider.Received(1).OpenDb2Stream("ChildTable");
        db2StreamProvider.DidNotReceive().OpenDb2Stream("AutoTable");
    }

    private static TestDbContext CreateContext()
    {
        var options = new DbContextOptionsBuilder<TestDbContext>()
            .UseInMemoryDatabase(Guid.NewGuid().ToString("N"))
            .Options;

        return new TestDbContext(options);
    }

    private static Db2Model CreateDb2ModelWithAutoIncludes()
    {
        var auto = new Dictionary<Type, IReadOnlyList<System.Reflection.MemberInfo>>
        {
            [typeof(Parent)] = new[] { typeof(Parent).GetProperty(nameof(Parent.Auto))! }
        };

        return new Db2Model(
            entityTypes: new Dictionary<Type, Db2EntityType>(),
            referenceNavigations: new Dictionary<(Type SourceClrType, System.Reflection.MemberInfo NavigationMember), Db2ReferenceNavigation>(),
            collectionNavigations: new Dictionary<(Type SourceClrType, System.Reflection.MemberInfo NavigationMember), Db2CollectionNavigation>(),
            autoIncludeNavigations: auto);
    }

    private static (IMimironDb2Store Store, IDb2StreamProvider Db2StreamProvider) CreateStore()
    {
        var db2StreamProvider = Substitute.For<IDb2StreamProvider>();
        db2StreamProvider.OpenDb2Stream(Arg.Any<string>()).Returns(_ => new MemoryStream());

        var dbdProvider = Substitute.For<IDbdProvider>();
        dbdProvider.Open(Arg.Any<string>()).Returns(_ => ParseDbd(TestDbd));

        var format = Substitute.For<IDb2Format>();
        format.OpenFile(Arg.Any<Stream>()).Returns(_ =>
        {
            var file = Substitute.For<IDb2File<RowHandle>>();
            file.RowType.Returns(typeof(RowHandle));
            return file;
        });

        format.GetLayout(Arg.Any<IDb2File>()).Returns(new Db2FileLayout(0x12345678, 5));

        var store = new MimironDb2Store(db2StreamProvider, dbdProvider, format, CreateStoreOptions());
        store.ShouldNotBeNull();

        return (store, db2StreamProvider);
    }

    private static IDbdFile ParseDbd(string text)
    {
        using var stream = new MemoryStream(Encoding.UTF8.GetBytes(text));
        return DbdFile.Parse(stream);
    }

    private static IDbContextOptions CreateStoreOptions()
    {
        var builder = new DbContextOptionsBuilder();

        var extension = new MimironDb2OptionsExtension().WithWowVersion(TestHelpers.WowVersion);
        ((IDbContextOptionsBuilderInfrastructure)builder).AddOrUpdateExtension(extension);

        return builder.Options;
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

    private sealed class TestDbContext(DbContextOptions<TestDbContext> options) : DbContext(options)
    {
        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<Parent>(b =>
            {
                b.ToTable("ParentTable");
                b.HasKey(x => x.Id);

                b.HasMany(x => x.Children)
                    .WithOne(x => x.Parent)
                    .HasForeignKey(x => x.ParentId);

                b.HasOne(x => x.Auto)
                    .WithMany()
                    .HasForeignKey(x => x.AutoId);

                b.Navigation(x => x.Auto).AutoInclude();
            });

            modelBuilder.Entity<Child>(b =>
            {
                b.ToTable("ChildTable");
                b.HasKey(x => x.Id);
            });

            modelBuilder.Entity<Auto>(b =>
            {
                b.ToTable("AutoTable");
                b.HasKey(x => x.Id);
            });
        }
    }

    private sealed class Parent
    {
        public int Id { get; set; }

        public int AutoId { get; set; }

        public Auto Auto { get; set; } = null!;

        public List<Child> Children { get; set; } = [];
    }

    private sealed class Child
    {
        public int Id { get; set; }

        public int ParentId { get; set; }

        public Parent Parent { get; set; } = null!;
    }

    private sealed class Auto
    {
        public int Id { get; set; }
    }
}
