using System.Text;

using MimironSQL.EntityFrameworkCore.Db2.Query;
using MimironSQL.Formats;

using NSubstitute;

using Shouldly;

namespace MimironSQL.EntityFrameworkCore.Tests;

public sealed class Db2DenseStringQueryTests
{
    public readonly struct TestRow;

    [Fact]
    public void Db2DenseStringMatch_FieldIndexNegative_ReturnsFalse()
    {
        var provider = Substitute.For<IDb2DenseStringTableIndexProvider<TestRow>>();
        HashSet<int> matchingStarts = [123];

        Db2DenseStringMatch.Contains(provider, default, fieldIndex: -1, matchingStarts).ShouldBeFalse();
        provider.DidNotReceive().TryGetDenseStringTableIndex(default, Arg.Any<int>(), out Arg.Any<int>());
    }

    [Fact]
    public void Db2DenseStringScanner_EmptyNeedle_ReturnsEmpty()
    {
        var bytes = new byte[] { (byte)'a', (byte)'b', 0 };
        Db2DenseStringScanner.FindStartOffsets(bytes, needle: string.Empty, Db2StringMatchKind.Contains).Count.ShouldBe(0);
    }

    [Fact]
    public void Db2DenseStringScanner_UnknownMatchKind_DoesNotAddAnyMatches()
    {
        var bytes = new byte[] { (byte)'a', (byte)'b', (byte)'c', 0 };
        var starts = Db2DenseStringScanner.FindStartOffsets(bytes, needle: "a", (Db2StringMatchKind)123);
        starts.Count.ShouldBe(0);
    }
}
