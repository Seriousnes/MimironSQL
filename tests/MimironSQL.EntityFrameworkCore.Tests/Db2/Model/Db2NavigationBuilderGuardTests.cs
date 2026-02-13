using System.Linq.Expressions;
using System.Reflection;

using MimironSQL.Db2;
using MimironSQL.EntityFrameworkCore.Db2.Model;
using MimironSQL.EntityFrameworkCore.Db2.Schema;

using Shouldly;

namespace MimironSQL.EntityFrameworkCore.Tests;

public sealed class Db2NavigationBuilderGuardTests
{
    [Fact]
    public void HasForeignKeyArray_throws_for_non_simple_member_access()
    {
        var ex = Should.Throw<NotSupportedException>(() =>
            _ = TestModelBindingFactory.CreateBinding(modelBuilder =>
            {
                modelBuilder.Entity<Principal>().HasKey(x => x.Id);
                modelBuilder.Entity<Dependent>().HasKey(x => x.Id);

                modelBuilder.Entity<Principal>()
                    .HasMany(x => x.Children)
                    .WithOne()
                    .HasForeignKeyArray(x => x.ChildIds.Where(i => i >= 0));
            }, SchemaResolver));

        ex.Message.ShouldContain("FK array selector");
    }

    [Fact]
    public void HasForeignKeyArray_throws_for_nested_member_access()
    {
        var ex = Should.Throw<NotSupportedException>(() =>
            _ = TestModelBindingFactory.CreateBinding(modelBuilder =>
            {
                modelBuilder.Entity<Principal>().HasKey(x => x.Id);
                modelBuilder.Entity<Dependent>().HasKey(x => x.Id);

                modelBuilder.Entity<Principal>()
                    .HasMany(x => x.Children)
                    .WithOne()
                    .HasForeignKeyArray(x => x.Inner.ChildIds);
            }, SchemaResolver));

        ex.Message.ShouldContain("direct member access");
    }

    [Fact]
    public void HasForeignKeyArray_throws_when_property_getter_is_not_public()
    {
        var ex = Should.Throw<NotSupportedException>(() =>
            _ = TestModelBindingFactory.CreateBinding(modelBuilder =>
            {
                modelBuilder.Entity<PrincipalPrivateGetter>().HasKey(x => x.Id);
                modelBuilder.Entity<Dependent>().HasKey(x => x.Id);

                var principal = modelBuilder.Entity<PrincipalPrivateGetter>();
                var rel = principal.HasMany(x => x.Children).WithOne();

                var param = Expression.Parameter(typeof(PrincipalPrivateGetter), "x");
                var prop = typeof(PrincipalPrivateGetter).GetProperty(nameof(PrincipalPrivateGetter.ChildIds), BindingFlags.Instance | BindingFlags.Public)!;
                var body = Expression.Property(param, prop);
                var lambda = Expression.Lambda<Func<PrincipalPrivateGetter, IEnumerable<int>>>(body, param);

                rel.HasForeignKeyArray(lambda);
            }, SchemaResolver));

        ex.Message.ShouldContain("must have a public getter");
    }

    private static Db2TableSchema SchemaResolver(string tableName)
        => new(
            tableName: tableName,
            layoutHash: 0,
            physicalColumnCount: 1,
            fields:
            [
                new Db2FieldSchema("ID", Db2ValueType.Int64, ColumnStartIndex: 0, ElementCount: 1, IsVerified: true, IsVirtual: false, IsId: true, IsRelation: false, ReferencedTableName: null),
            ]);

    private sealed class Principal
    {
        public int Id { get; set; }

        public int[] ChildIds { get; set; } = [];

        public Inner Inner { get; set; } = new();

        public ICollection<Dependent> Children { get; set; } = [];
    }

    private sealed class PrincipalPrivateGetter
    {
        public int Id { get; set; }

        public int[] ChildIds { private get; set; } = [];

        public ICollection<Dependent> Children { get; set; } = [];
    }

    private sealed class Inner
    {
        public int[] ChildIds { get; set; } = [];
    }

    private sealed class Dependent
    {
        public int Id { get; set; }
    }
}
