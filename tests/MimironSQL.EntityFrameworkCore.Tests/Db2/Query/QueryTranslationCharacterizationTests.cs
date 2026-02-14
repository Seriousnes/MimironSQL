using Microsoft.EntityFrameworkCore;

using Microsoft.Extensions.DependencyInjection;

using Shouldly;

using MimironSQL.Db2;
using MimironSQL.Dbd;
using MimironSQL.EntityFrameworkCore.Infrastructure;
using MimironSQL.Formats;
using MimironSQL.Providers;

namespace MimironSQL.EntityFrameworkCore.Tests.Db2.Query;

public sealed class QueryTranslationCharacterizationTests
{
    [Fact]
    public void FirstOrDefault_throws_translation_not_implemented_yet()
    {
        using var built = CreateContext<TranslationContext>();
        var ex = Should.Throw<NotSupportedException>(() => built.Context.Entities.FirstOrDefault());
        ex.Message.ShouldBe("MimironDb2 query translation is not implemented yet.");
    }

    [Fact]
    public void SingleOrDefault_throws_translation_not_implemented_yet()
    {
        using var built = CreateContext<TranslationContext>();
        var ex = Should.Throw<NotSupportedException>(() => built.Context.Entities.SingleOrDefault());
        ex.Message.ShouldBe("MimironDb2 query translation is not implemented yet.");
    }

    [Fact]
    public void Any_throws_translation_not_implemented_yet()
    {
        using var built = CreateContext<TranslationContext>();
        var ex = Should.Throw<NotSupportedException>(() => built.Context.Entities.Any());
        ex.Message.ShouldBe("MimironDb2 query translation is not implemented yet.");
    }

    [Fact]
    public void Count_throws_translation_not_implemented_yet()
    {
        using var built = CreateContext<TranslationContext>();
        var ex = Should.Throw<NotSupportedException>(() => built.Context.Entities.Count());
        ex.Message.ShouldBe("MimironDb2 query translation is not implemented yet.");
    }

    [Fact]
    public void Queryable_contains_throws_translation_not_implemented_yet()
    {
        using var built = CreateContext<TranslationContext>();
        var ex = Should.Throw<NotSupportedException>(() => built.Context.Entities.Select(e => e.Id).Contains(1));
        ex.Message.ShouldBe("MimironDb2 query translation is not implemented yet.");
    }

    [Fact]
    public void Where_and_Take_can_execute_when_AsNoTracking_and_empty_source()
    {
        using var built = CreateContext<TranslationContext>();

        // This is mainly a smoke test for the one path we claim is supported today (Where + Take),
        // while avoiding the tracking-related NullReferenceException.
        var results = built.Context.Entities
            .AsNoTracking()
            .Where(e => e.Id > 0)
            .Take(1)
            .ToList();

        results.Count.ShouldBe(0);
    }

    private static BuiltContext<TContext> CreateContext<TContext>() where TContext : DbContext
    {
        var optionsBuilder = new DbContextOptionsBuilder<TContext>();

        optionsBuilder.UseMimironDb2ForTests(o =>
        {
            o.ConfigureProvider(
                providerKey: "test",
                providerConfigHash: 0,
                applyProviderServices: services =>
                {
                    services.AddSingleton<IDbdProvider>(new EmptyDbdProvider());
                    services.AddSingleton<IDb2StreamProvider>(new EmptyDb2StreamProvider());
                    services.AddSingleton<IDb2Format>(new EmptyDb2Format());
                });
        });

        var extension = optionsBuilder.Options.FindExtension<MimironDb2OptionsExtension>();
        extension.ShouldNotBeNull();

        var services = new Microsoft.Extensions.DependencyInjection.ServiceCollection();
        extension!.ApplyServices(services);

        var serviceProvider = services.BuildServiceProvider();
        optionsBuilder.UseInternalServiceProvider(serviceProvider);

        var context = (TContext)Activator.CreateInstance(typeof(TContext), optionsBuilder.Options)!;
        return new BuiltContext<TContext>(context, serviceProvider);
    }

    private sealed record BuiltContext<TContext>(TContext Context, IServiceProvider ServiceProvider) : IDisposable
        where TContext : DbContext
    {
        public void Dispose()
        {
            Context.Dispose();
            (ServiceProvider as IDisposable)?.Dispose();
        }
    }

    private sealed class TranslationContext(DbContextOptions<TranslationContext> options) : DbContext(options)
    {
        public DbSet<TranslationEntity> Entities => Set<TranslationEntity>();

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<TranslationEntity>(b =>
            {
                b.HasKey(e => e.Id);
                b.Property(e => e.Name);
            });
        }
    }

    private sealed class TranslationEntity
    {
        public int Id { get; set; }
        public string? Name { get; set; }
    }

    private sealed class EmptyDbdProvider : IDbdProvider
    {
        public IDbdFile Open(string tableName)
            => throw new InvalidOperationException("EmptyDbdProvider should not be used by translation-only tests.");
    }

    private sealed class EmptyDb2StreamProvider : IDb2StreamProvider
    {
        public Stream OpenDb2Stream(string tableName) => new MemoryStream();

        public Task<Stream> OpenDb2StreamAsync(string tableName, CancellationToken cancellationToken = default)
            => Task.FromResult<Stream>(new MemoryStream());
    }

    private sealed class EmptyDb2Format : IDb2Format
    {
        public Db2Format Format => Db2Format.Unknown;

        public IDb2File OpenFile(Stream stream) => new EmptyDb2File();

        public Db2FileLayout GetLayout(IDb2File file) => new(layoutHash: 0, physicalFieldsCount: 0);

        public Db2FileLayout GetLayout(Stream stream) => new(layoutHash: 0, physicalFieldsCount: 0);

        private sealed class EmptyDb2File : IDb2File<RowHandle>
        {
            public IDb2FileHeader Header { get; } = new EmptyHeader();
            public Type RowType => typeof(RowHandle);
            public Db2Flags Flags => 0;
            public int RecordsCount => 0;
            public ReadOnlyMemory<byte> DenseStringTableBytes => ReadOnlyMemory<byte>.Empty;

            public IEnumerable<RowHandle> EnumerateRowHandles() => [];

            public IEnumerable<RowHandle> EnumerateRows() => [];

            public T ReadField<T>(RowHandle handle, int fieldIndex) => default!;

            public bool TryGetRowHandle<TId>(TId id, out RowHandle handle) where TId : IEquatable<TId>, IComparable<TId>
            {
                handle = default;
                return false;
            }

            public bool TryGetRowById<TId>(TId id, out RowHandle row) where TId : IEquatable<TId>, IComparable<TId>
            {
                row = default;
                return false;
            }

            public void Dispose()
            {
            }

            private sealed class EmptyHeader : IDb2FileHeader
            {
                public uint LayoutHash => 0;
                public int FieldsCount => 0;
            }
        }
    }
}
