using System.ComponentModel.DataAnnotations.Schema;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.EntityFrameworkCore;
using MimironSQL.EntityFrameworkCore;
using MimironSQL.Providers;

using NSubstitute;

using Shouldly;

namespace MimironSQL.EntityFrameworkCore.Tests;

public sealed class MimironDb2ModelCustomizerTests
{
    [Fact]
    public void Customize_applies_table_and_column_attribute_names()
    {
        var db2Provider = Substitute.For<IDb2StreamProvider>();
        var dbdProvider = Substitute.For<IDbdProvider>();

        var options = new DbContextOptionsBuilder<TestContext>()
            .UseMimironDb2(o => o.ConfigureProvider(
                providerKey: "Test",
                providerConfigHash: 1,
                applyProviderServices: services =>
                {
                    services.AddSingleton<IDb2StreamProvider>(db2Provider);
                    services.AddSingleton<IDbdProvider>(dbdProvider);
                }))
            .Options;

        using var context = new TestContext(options);

        var entityType = context.Model.FindEntityType(typeof(EntityWithAttributes));
        entityType.ShouldNotBeNull();

        entityType!.GetTableName().ShouldBe("MyTable");
        entityType.FindProperty(nameof(EntityWithAttributes.Id))!.GetColumnName().ShouldBe("MyId");
        entityType.FindProperty(nameof(EntityWithAttributes.Name))!.GetColumnName().ShouldBe(nameof(EntityWithAttributes.Name));
    }

    private sealed class TestContext(DbContextOptions options) : DbContext(options)
    {
        public DbSet<EntityWithAttributes> Entities => Set<EntityWithAttributes>();
    }

    [Table("MyTable")]
    private sealed class EntityWithAttributes
    {
        [Column("MyId")]
        public int Id { get; set; }

        public string Name { get; set; } = string.Empty;
    }
}
