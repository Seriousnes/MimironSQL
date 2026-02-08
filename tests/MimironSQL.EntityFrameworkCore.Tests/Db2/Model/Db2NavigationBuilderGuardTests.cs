using System;
using System.Collections.Generic;
using System.Linq.Expressions;

using MimironSQL.Db2.Model;

using Shouldly;

namespace MimironSQL.EntityFrameworkCore.Tests;

public sealed class Db2NavigationBuilderGuardTests
{
    [Fact]
    public void HasOne_throws_for_field_navigation()
    {
        var builder = new Db2ModelBuilder();

        var ex = Should.Throw<NotSupportedException>(() =>
            builder.Entity<HasOneFieldSource>().HasOne(x => x.Parent));

        ex.Message.ShouldContain("HasOne");
        ex.Message.ShouldContain("property");
    }

    [Fact]
    public void HasOne_throws_for_nested_member_access()
    {
        var builder = new Db2ModelBuilder();

        var ex = Should.Throw<NotSupportedException>(() =>
            builder.Entity<HasOneNestedSource>().HasOne(x => x.Inner.Parent));

        ex.Message.ShouldContain("direct member access");
    }

    [Fact]
    public void HasOne_throws_for_non_public_getter()
    {
        var builder = new Db2ModelBuilder();

        var entity = builder.Entity<HasOnePrivateGetterSource>();

        var param = Expression.Parameter(typeof(HasOnePrivateGetterSource), "x");
        var prop = typeof(HasOnePrivateGetterSource).GetProperty(nameof(HasOnePrivateGetterSource.Parent))!;
        var body = Expression.Property(param, prop);
        var lambda = Expression.Lambda<Func<HasOnePrivateGetterSource, HasOneTarget?>>(body, param);

        var ex = Should.Throw<NotSupportedException>(() => entity.HasOne(lambda));
        ex.Message.ShouldContain("public getter");
    }

    [Fact]
    public void HasOne_throws_for_navigation_type_mismatch()
    {
        var builder = new Db2ModelBuilder();

        var entity = builder.Entity<HasOneObjectNavSource>();

        Expression<Func<HasOneObjectNavSource, HasOneTarget?>> nav = x => (HasOneTarget?)x.Parent;

        var ex = Should.Throw<NotSupportedException>(() => entity.HasOne(nav));
        ex.Message.ShouldContain("Navigation type mismatch");
    }

    [Fact]
    public void HasMany_throws_for_field_navigation()
    {
        var builder = new Db2ModelBuilder();

        var ex = Should.Throw<NotSupportedException>(() =>
            builder.Entity<HasManyFieldSource>().HasMany(x => x.Children));

        ex.Message.ShouldContain("HasMany");
        ex.Message.ShouldContain("property");
    }

    [Fact]
    public void HasMany_throws_for_nested_member_access()
    {
        var builder = new Db2ModelBuilder();

        var ex = Should.Throw<NotSupportedException>(() =>
            builder.Entity<HasManyNestedSource>().HasMany(x => x.Inner.Children));

        ex.Message.ShouldContain("direct member access");
    }

    [Fact]
    public void HasMany_throws_for_non_public_getter()
    {
        var builder = new Db2ModelBuilder();

        var entity = builder.Entity<HasManyPrivateGetterSource>();

        var param = Expression.Parameter(typeof(HasManyPrivateGetterSource), "x");
        var prop = typeof(HasManyPrivateGetterSource).GetProperty(nameof(HasManyPrivateGetterSource.Children))!;
        var body = Expression.Property(param, prop);
        var lambda = Expression.Lambda<Func<HasManyPrivateGetterSource, ICollection<HasManyTarget>>>(body, param);

        var ex = Should.Throw<NotSupportedException>(() => entity.HasMany(lambda));
        ex.Message.ShouldContain("public getter");
    }

    [Fact]
    public void HasMany_throws_for_navigation_type_mismatch()
    {
        var builder = new Db2ModelBuilder();

        var entity = builder.Entity<HasManyObjectNavSource>();

        Expression<Func<HasManyObjectNavSource, ICollection<HasManyTarget>>> nav = x => (ICollection<HasManyTarget>)x.Children;

        var ex = Should.Throw<NotSupportedException>(() => entity.HasMany(nav));
        ex.Message.ShouldContain("Navigation type mismatch");
    }

    [Fact]
    public void Reference_navigation_builders_validate_key_selectors()
    {
        var builder = new Db2ModelBuilder();

        var refNav = builder.Entity<RefNavSource>()
            .HasOne(x => x.Parent);

        Should.Throw<NotSupportedException>(() => refNav.WithForeignKey(x => x.ParentId + 1))
            .Message.ShouldContain("FK selector");

        var nestedFk = Should.Throw<NotSupportedException>(() => refNav.WithForeignKey(x => x.Inner.ParentId));
        nestedFk.Message.ShouldContain("direct member access");

        var pk = Should.Throw<NotSupportedException>(() => refNav.HasPrincipalKey(x => x.Id + 1));
        pk.Message.ShouldContain("Principal key selector");
    }

    [Fact]
    public void Collection_navigation_builders_validate_key_selectors()
    {
        var builder = new Db2ModelBuilder();

        var collNav = builder.Entity<CollNavSource>()
            .HasMany(x => x.Children);

        Should.Throw<NotSupportedException>(() => collNav.WithForeignKeyArray(x => x.ChildIds.Where(i => i >= 0)))
            .Message.ShouldContain("FK array selector");

        var nested = Should.Throw<NotSupportedException>(() => collNav.WithForeignKeyArray(x => x.Inner.ChildIds));
        nested.Message.ShouldContain("direct member access");

        var principal = Should.Throw<NotSupportedException>(() => collNav.HasPrincipalKey(x => x.Id + 1));
        principal.Message.ShouldContain("Principal key selector");

        var dependentFk = Should.Throw<NotSupportedException>(() => collNav.WithForeignKey(c => c.ParentId + 1));
        dependentFk.Message.ShouldContain("FK selector");
    }

    [Fact]
    public void Builder_key_selector_throws_when_property_getter_is_not_public()
    {
        var builder = new Db2ModelBuilder();

        var refNav = builder.Entity<RefNavSource>()
            .HasOne(x => x.Parent);

        var param = Expression.Parameter(typeof(RefNavSource), "x");
        var fkProp = typeof(RefNavSource).GetProperty(nameof(RefNavSource.PrivateGetterParentId))!;
        var body = Expression.Property(param, fkProp);
        var lambda = Expression.Lambda<Func<RefNavSource, int>>(body, param);

        var ex = Should.Throw<NotSupportedException>(() => refNav.WithForeignKey(lambda));
        ex.Message.ShouldContain("must have a public getter");

        var collNav = builder.Entity<CollNavSource>()
            .HasMany(x => x.Children);

        var childParam = Expression.Parameter(typeof(CollNavTarget), "c");
        var childFkProp = typeof(CollNavTarget).GetProperty(nameof(CollNavTarget.PrivateGetterParentId))!;
        var childBody = Expression.Property(childParam, childFkProp);
        var childLambda = Expression.Lambda<Func<CollNavTarget, int>>(childBody, childParam);

        var ex2 = Should.Throw<NotSupportedException>(() => collNav.WithForeignKey(childLambda));
        ex2.Message.ShouldContain("must have a public getter");
    }

    private sealed class HasOneTarget;

    private sealed class HasOneUnrelated;

    private sealed class HasOneFieldSource
    {
        public HasOneTarget? Parent;
    }

    private sealed class HasOneInner
    {
        public HasOneTarget? Parent { get; set; }
    }

    private sealed class HasOneNestedSource
    {
        public HasOneInner Inner { get; set; } = new();
    }

    private sealed class HasOnePrivateGetterSource
    {
        public HasOneTarget? Parent { private get; set; }
    }

    private sealed class HasOneMismatchSource
    {
        public HasOneTarget? Parent { get; set; }
    }

    private sealed class HasOneObjectNavSource
    {
        public object? Parent { get; set; }
    }

    private sealed class HasManyTarget;

    private sealed class HasManyUnrelated;

    private sealed class HasManyFieldSource
    {
        public readonly ICollection<HasManyTarget> Children = [];
    }

    private sealed class HasManyInner
    {
        public ICollection<HasManyTarget> Children { get; set; } = [];
    }

    private sealed class HasManyNestedSource
    {
        public HasManyInner Inner { get; set; } = new();
    }

    private sealed class HasManyPrivateGetterSource
    {
        public ICollection<HasManyTarget> Children { private get; set; } = [];
    }

    private sealed class HasManyMismatchSource
    {
        public ICollection<HasManyTarget> Children { get; set; } = [];
    }

    private sealed class HasManyObjectNavSource
    {
        public IEnumerable<object> Children { get; set; } = Array.Empty<object>();
    }

    private sealed class RefNavTarget
    {
        public int Id { get; set; }
    }

    private sealed class RefNavInner
    {
        public int ParentId { get; set; }
    }

    private sealed class RefNavSource
    {
        public int ParentId { get; set; }

        public int PrivateGetterParentId { private get; set; }

        public RefNavInner Inner { get; set; } = new();

        public RefNavTarget? Parent { get; set; }
    }

    private sealed class CollNavTarget
    {
        public int ParentId { get; set; }

        public int PrivateGetterParentId { private get; set; }
    }

    private sealed class CollNavInner
    {
        public IEnumerable<int> ChildIds { get; set; } = Array.Empty<int>();
    }

    private sealed class CollNavSource
    {
        public int Id { get; set; }

        public IEnumerable<int> ChildIds { get; set; } = Array.Empty<int>();

        public CollNavInner Inner { get; set; } = new();

        public ICollection<CollNavTarget> Children { get; set; } = [];
    }

    private sealed class HasManyMismatchSource2
    {
        public IEnumerable<HasManyTarget?> Children { get; set; } = Array.Empty<HasManyTarget?>();
    }
}
