using System.Linq.Expressions;
using System.Reflection;

using MimironSQL.Extensions;

using Shouldly;

namespace MimironSQL.EntityFrameworkCore.Tests;

public sealed class ExtensionsTests
{
    [Fact]
    public void ExpressionExtensions_UnwrapConvert_Convert_UnwrapsOperand()
    {
        var p = Expression.Parameter(typeof(int), "x");
        var convert = Expression.ConvertChecked(p, typeof(long));

        convert.UnwrapConvert().ShouldBeSameAs(p);
    }

    [Fact]
    public void ExpressionExtensions_UnwrapConvert_NonConvert_ReturnsExpression()
    {
        var p = Expression.Parameter(typeof(int), "x");

        p.UnwrapConvert().ShouldBeSameAs(p);
        ((Expression?)null).UnwrapConvert().ShouldBeNull();
    }

    [Fact]
    public void ExpressionExtensions_ContainsParameter_FindsParameter_InMemberAccess()
    {
        var p = Expression.Parameter(typeof(KeyHolder), "h");
        var body = Expression.Property(p, nameof(KeyHolder.IntKey));

        body.ContainsParameter(p).ShouldBeTrue();
    }

    [Fact]
    public void ExpressionExtensions_ContainsParameter_FindsParameter_InCompositeExpressionGraph()
    {
        var p = Expression.Parameter(typeof(KeyHolder), "h");

        var intKey = Expression.Property(p, nameof(KeyHolder.IntKey));
        var longKey = Expression.Property(p, nameof(KeyHolder.LongKey));
        var toString = Expression.Call(longKey, typeof(long).GetMethod(nameof(long.ToString), Type.EmptyTypes)!);
        var concat = Expression.Call(typeof(string).GetMethod(nameof(string.Concat), [typeof(string), typeof(string)])!, Expression.Constant("k="), toString);

        var condition = Expression.Condition(
            test: Expression.GreaterThan(intKey, Expression.Constant(0)),
            ifTrue: concat,
            ifFalse: Expression.Constant("none"));

        var init = Expression.MemberInit(
            Expression.New(typeof(Holder).GetConstructor(Type.EmptyTypes)!),
            Expression.Bind(typeof(Holder).GetProperty(nameof(Holder.Value))!, condition));

        init.ContainsParameter(p).ShouldBeTrue();
    }

    [Fact]
    public void ExpressionExtensions_ContainsParameter_MissingParameter_ReturnsFalse()
    {
        var p = Expression.Parameter(typeof(KeyHolder), "h");
        var other = Expression.Parameter(typeof(KeyHolder), "o");
        var body = Expression.Property(other, nameof(KeyHolder.IntKey));

        body.ContainsParameter(p).ShouldBeFalse();
    }

    [Fact]
    public void TypeExtensions_IsScalarType_RecognizesPrimitivesEnumsAndNullable()
    {
        typeof(int).IsScalarType().ShouldBeTrue();
        typeof(int?).IsScalarType().ShouldBeTrue();
        typeof(TestEnum).IsScalarType().ShouldBeTrue();

        typeof(string).IsScalarType().ShouldBeFalse();
        typeof(Guid).IsScalarType().ShouldBeFalse();
    }

    [Fact]
    public void TypeExtensions_IsNullableAndUnwrapNullable_Work()
    {
        typeof(int?).IsNullable().ShouldBeTrue();
        typeof(int).IsNullable().ShouldBeFalse();

        typeof(int?).UnwrapNullable().ShouldBe(typeof(int));
        typeof(string).UnwrapNullable().ShouldBe(typeof(string));
    }

    [Fact]
    public void Db2QueryExtensions_MatchesSharedPrimaryKeyNullCheck_ImplementsNullSemantics()
    {
        HashSet<int> existing = [10];

        existing.MatchesSharedPrimaryKeyNullCheck(id: 0, isNotNull: true).ShouldBeFalse();
        existing.MatchesSharedPrimaryKeyNullCheck(id: 10, isNotNull: true).ShouldBeTrue();
        existing.MatchesSharedPrimaryKeyNullCheck(id: 11, isNotNull: true).ShouldBeFalse();

        existing.MatchesSharedPrimaryKeyNullCheck(id: 0, isNotNull: false).ShouldBeTrue();
        existing.MatchesSharedPrimaryKeyNullCheck(id: 10, isNotNull: false).ShouldBeFalse();
        existing.MatchesSharedPrimaryKeyNullCheck(id: 11, isNotNull: false).ShouldBeTrue();
    }

    [Fact]
    public void MemberInfoExtensions_GetMemberType_ReturnsFieldOrPropertyType()
    {
        typeof(KeyHolder).GetProperty(nameof(KeyHolder.IntKey))!.GetMemberType().ShouldBe(typeof(int));
        typeof(KeyHolder).GetField(nameof(KeyHolder.PublicField))!.GetMemberType().ShouldBe(typeof(int));

        var ex = Should.Throw<InvalidOperationException>(() => typeof(KeyHolder).GetMethod(nameof(KeyHolder.Method))!.GetMemberType());
        ex.Message.ShouldContain("Unexpected member type");
    }

    [Fact]
    public void Db2KeyExpressionExtensions_CreateInt32KeyExpression_ConvertsSupportedTypes()
    {
        var p = Expression.Parameter(typeof(KeyHolder), "h");

        AssertKey(nameof(KeyHolder.IntKey), new KeyHolder { IntKey = 7 }, expected: 7);
        AssertKey(nameof(KeyHolder.LongKey), new KeyHolder { LongKey = 8 }, expected: 8);
        AssertKey(nameof(KeyHolder.UIntKey), new KeyHolder { UIntKey = 9 }, expected: 9);
        AssertKey(nameof(KeyHolder.ULongKey), new KeyHolder { ULongKey = 10 }, expected: 10);
        AssertKey(nameof(KeyHolder.ShortKey), new KeyHolder { ShortKey = 11 }, expected: 11);
        AssertKey(nameof(KeyHolder.ByteKey), new KeyHolder { ByteKey = 12 }, expected: 12);
        AssertKey(nameof(KeyHolder.EnumKey), new KeyHolder { EnumKey = TestEnum.B }, expected: (int)TestEnum.B);

        void AssertKey(string propertyName, KeyHolder holder, int expected)
        {
            var prop = typeof(KeyHolder).GetProperty(propertyName)!;
            var expr = prop.CreateInt32KeyExpression(p);
            var lambda = Expression.Lambda<Func<KeyHolder, int>>(expr, p).Compile();
            lambda(holder).ShouldBe(expected);
        }
    }

    [Fact]
    public void Db2KeyExpressionExtensions_CreateInt32KeyExpression_UnsupportedMemberType_Throws()
    {
        var p = Expression.Parameter(typeof(KeyHolder), "h");
        var prop = typeof(KeyHolder).GetProperty(nameof(KeyHolder.GuidKey))!;

        var ex = Should.Throw<NotSupportedException>(() => prop.CreateInt32KeyExpression(p));
        ex.Message.ShouldContain("Unsupported key member type");
    }

    [Fact]
    public void Db2KeyExpressionExtensions_CreateInt32KeyExpression_FieldMember_Throws()
    {
        var p = Expression.Parameter(typeof(KeyHolder), "h");
        var field = typeof(KeyHolder).GetField(nameof(KeyHolder.PublicField))!;

        var ex = Should.Throw<NotSupportedException>(() => field.CreateInt32KeyExpression(p));
        ex.Message.ShouldContain("must be a public property");
    }

    private sealed class Holder
    {
        public string Value { get; set; } = string.Empty;
    }

    private sealed class KeyHolder
    {
        public int IntKey { get; init; }
        public long LongKey { get; init; }
        public uint UIntKey { get; init; }
        public ulong ULongKey { get; init; }
        public short ShortKey { get; init; }
        public byte ByteKey { get; init; }
        public TestEnum EnumKey { get; init; }
        public Guid GuidKey { get; init; }

        public int PublicField;

        public void Method()
        {
        }
    }

    private enum TestEnum : ushort
    {
        A = 1,
        B = 2,
    }
}
