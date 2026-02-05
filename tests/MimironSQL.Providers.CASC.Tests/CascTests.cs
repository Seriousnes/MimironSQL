using Shouldly;

namespace MimironSQL.Providers.CASC.Tests;

public sealed class CascTests
{
    [Fact]
    public void CascPath_NormalizeCascPath_RewritesSeparatorsAndLeadingSlash()
    {
        CascPath.NormalizeCascPath("\\DBFilesClient/Spell.db2").ShouldBe("DBFilesClient\\Spell.db2");
    }

    [Fact]
    public void CascPath_NormalizeDb2Path_EnforcesPrefixAndCanonicalizes()
    {
        CascPath.NormalizeDb2Path("dbfilesclient/spell.db2").ShouldBe("DBFilesClient\\spell.db2");
        Should.Throw<ArgumentException>(() => CascPath.NormalizeDb2Path("notdbfiles/spell.db2"));
    }

    [Fact]
    public void CascBucket_GetBucketIndex_RequiresAtLeastNineBytes()
    {
        Should.Throw<ArgumentException>(() => CascBucket.GetBucketIndex([1, 2, 3, 4, 5, 6, 7, 8]));
    }

    [Fact]
    public void CascBucket_GetBucketIndexCrossReference_IsWithin0To15()
    {
        var idx = CascBucket.GetBucketIndexCrossReference(new byte[16]);
        idx.ShouldBeLessThan((byte)16);
    }

    [Fact]
    public void WowBuildVersion_TryParse_ParsesExpectedFormat()
    {
        WowBuildVersion.TryParse("11.0.2.58712", out var v).ShouldBeTrue();
        v.Major.ShouldBe(11);
        v.Minor.ShouldBe(0);
        v.Patch.ShouldBe(2);
        v.Build.ShouldBe(58712);
    }

    [Fact]
    public void WowBuildIdentityProvider_TryParseBuildNumber_FindsLastDigitRun()
    {
        WowBuildIdentityProvider.TryParseBuildNumber("11.0.2.58712").ShouldBe(58712);
        WowBuildIdentityProvider.TryParseBuildNumber("foo 123 bar 456").ShouldBe(456);
        WowBuildIdentityProvider.TryParseBuildNumber("no digits").ShouldBeNull();
    }

    [Fact]
    public void WowBuildIdentityProvider_SanitizeForNamespace_RewritesNonDigitsAndCollapses()
    {
        WowBuildIdentityProvider.SanitizeForNamespace("11.0.2.58712").ShouldBe("11_0_2_58712");
        WowBuildIdentityProvider.SanitizeForNamespace("---").ShouldBe("unknown");
    }

    [Fact]
    public void CascKey_ParseHex_RoundTrips()
    {
        var hex = "000102030405060708090a0b0c0d0e0f";
        var key = CascKey.ParseHex(hex);
        key.ToString().ShouldBe(hex);
        key.ToByteArray().Length.ShouldBe(CascKey.Length);
        (key == CascKey.ParseHex(hex)).ShouldBeTrue();
        (key != CascKey.ParseHex("000102030405060708090a0b0c0d0e00")).ShouldBeTrue();
    }

    [Fact]
    public void EndianBitConverter_ReadUIntBigEndian_ReadsUpToEightBytes()
    {
        EndianBitConverter.ReadUIntBigEndian([0x01]).ShouldBe(1UL);
        EndianBitConverter.ReadUIntBigEndian([0x01, 0x02]).ShouldBe(0x0102UL);
        Should.Throw<ArgumentOutOfRangeException>(() => EndianBitConverter.ReadUIntBigEndian(ReadOnlySpan<byte>.Empty));
        Should.Throw<ArgumentOutOfRangeException>(() => EndianBitConverter.ReadUIntBigEndian(new byte[9]));
    }
}
