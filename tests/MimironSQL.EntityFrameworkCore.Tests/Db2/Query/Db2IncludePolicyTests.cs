using System.Linq.Expressions;
using System.Reflection;

using MimironSQL.Db2;
using MimironSQL.EntityFrameworkCore.Db2.Model;
using MimironSQL.EntityFrameworkCore.Db2.Query;
using MimironSQL.EntityFrameworkCore.Db2.Schema;

using Shouldly;

namespace MimironSQL.EntityFrameworkCore.Tests;

public sealed class Db2IncludePolicyTests
{
    [Fact]
    public void UsesRootNavigation_returns_true_when_lambda_reads_root_navigation_member()
    {
        var model = CreateModel();

        Expression<Func<Child, string>> lambda = x => x.Parent!.Name;
        Db2IncludePolicy.UsesRootNavigation(model, lambda).ShouldBeTrue();
    }

    [Fact]
    public void ThrowIfNavigationRequiresInclude_throws_when_nav_is_used_without_include()
    {
        var model = CreateModel();
        var includedRootMembers = new HashSet<MemberInfo>();

        Expression<Func<Child, string>> lambda = x => x.Parent!.Name.ToUpperInvariant();

        Should.Throw<NotSupportedException>(() => Db2IncludePolicy.ThrowIfNavigationRequiresInclude(model, includedRootMembers, lambda))
            .Message.ShouldContain("requires an explicit Include");
    }

    [Fact]
    public void ThrowIfNavigationRequiresInclude_does_not_throw_when_nav_is_included()
    {
        var model = CreateModel();
        var navMember = typeof(Child).GetProperty(nameof(Child.Parent), BindingFlags.Instance | BindingFlags.Public)!;

        var includedRootMembers = new HashSet<MemberInfo> { navMember };

        Expression<Func<Child, int>> lambda = x => x.Parent != null ? x.Parent.Id : 0;

        Db2IncludePolicy.ThrowIfNavigationRequiresInclude(model, includedRootMembers, lambda);
    }

    [Fact]
    public void ThrowIfNavigationRequiresInclude_supports_Db2WhereOperation_and_Db2SelectOperation_overloads()
    {
        var model = CreateModel();
        var includedRootMembers = new HashSet<MemberInfo>();

        Expression<Func<Child, bool>> where = x => x.Parent != null;
        Should.Throw<NotSupportedException>(() => Db2IncludePolicy.ThrowIfNavigationRequiresInclude(model, includedRootMembers, new Db2WhereOperation(where)));

        Expression<Func<Child, string>> select = x => x.Parent!.Name;
        Should.Throw<NotSupportedException>(() => Db2IncludePolicy.ThrowIfNavigationRequiresInclude(model, includedRootMembers, new Db2SelectOperation(select)));
    }

    [Fact]
    public void ThrowIfNavigationRequiresInclude_handles_multiple_mentions_and_dedupes_root_navigation_member()
    {
        var model = CreateModel();
        var navMember = typeof(Child).GetProperty(nameof(Child.Parent), BindingFlags.Instance | BindingFlags.Public)!;

        var includedRootMembers = new HashSet<MemberInfo> { navMember };

        Expression<Func<Child, Holder>> lambda = x => new Holder
        {
            Name1 = x.Parent!.Name,
            Name2 = x.Parent!.Name,
        };

        Db2IncludePolicy.ThrowIfNavigationRequiresInclude(model, includedRootMembers, lambda);
    }

    private static Db2Model CreateModel()
    {
        var builder = new Db2ModelBuilder();

        builder.Entity<Child>()
            .HasOne(x => x.Parent)
            .WithForeignKey(x => x.ParentId);

        builder.Entity<Parent>().HasKey(x => x.Id);
        builder.Entity<Child>().HasKey(x => x.Id);

        return builder.Build(SchemaResolver);
    }

    private static Db2TableSchema SchemaResolver(string tableName)
        => tableName switch
        {
            nameof(Parent) => new Db2TableSchema(
                tableName: nameof(Parent),
                layoutHash: 0,
                physicalColumnCount: 2,
                fields:
                [
                    new Db2FieldSchema("ID", Db2ValueType.Int64, ColumnStartIndex: 0, ElementCount: 1, IsVerified: true, IsVirtual: false, IsId: true, IsRelation: false, ReferencedTableName: null),
                    new Db2FieldSchema(nameof(Parent.Name), Db2ValueType.String, ColumnStartIndex: 1, ElementCount: 1, IsVerified: true, IsVirtual: false, IsId: false, IsRelation: false, ReferencedTableName: null),
                ]),

            nameof(Child) => new Db2TableSchema(
                tableName: nameof(Child),
                layoutHash: 0,
                physicalColumnCount: 2,
                fields:
                [
                    new Db2FieldSchema("ID", Db2ValueType.Int64, ColumnStartIndex: 0, ElementCount: 1, IsVerified: true, IsVirtual: false, IsId: true, IsRelation: false, ReferencedTableName: null),
                    new Db2FieldSchema(nameof(Child.ParentId), Db2ValueType.Int64, ColumnStartIndex: 1, ElementCount: 1, IsVerified: true, IsVirtual: false, IsId: false, IsRelation: false, ReferencedTableName: nameof(Parent)),
                ]),

            _ => throw new InvalidOperationException($"Unknown table: {tableName}"),
        };

    private sealed class Parent
    {
        public int Id { get; set; }

        public string Name { get; set; } = string.Empty;
    }

    private sealed class Child
    {
        public int Id { get; set; }

        public int ParentId { get; set; }

        public Parent? Parent { get; set; }
    }

    private sealed class Holder
    {
        public string Name1 { get; set; } = string.Empty;

        public string Name2 { get; set; } = string.Empty;
    }
}
