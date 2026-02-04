using System.Text;

using Shouldly;

using MimironSQL.Dbd;
using MimironSQL.Db2;

namespace MimironSQL.Dbd.Tests;

public sealed class DbdTests
{
    [Fact]
    public void DbdLayout_ParseHeader_Empty_ReturnsEmptyHashes()
    {
        var layout = DbdLayout.ParseHeader("LAYOUT ");
        layout.Hashes.Length.ShouldBe(0);
    }

    [Fact]
    public void DbdLayout_ParseHeader_TwoHashes_ParsesHex()
    {
        var layout = DbdLayout.ParseHeader("LAYOUT 2273DFFF, 60BB6C3F");
        layout.Hashes.ShouldBe([0x2273DFFF, 0x60BB6C3F]);
    }

    [Fact]
    public void DbdColumnParser_TryParse_ReferenceTable_ParsesReferenceAndVerifiedFlag()
    {
        DbdColumnParser.TryParse("int<Map::ID> ParentMapID?", out var name, out var col).ShouldBeTrue();
        name.ShouldBe("ParentMapID");
        col.ValueType.ShouldBe(Db2ValueType.Int64);
        col.ReferencedTableName.ShouldBe("Map");
        col.IsVerified.ShouldBeFalse();
    }

    [Fact]
    public void DbdLayoutEntryParser_TryParse_ModifiersAndArray_ParsesFlagsAndCount()
    {
        var columns = new Dictionary<string, DbdColumn>(StringComparer.Ordinal)
        {
            ["Name"] = new DbdColumn(Db2ValueType.String, referencedTableName: null, isVerified: true),
        };

        DbdLayoutEntryParser.TryParse("$noninline,relation$ Name[3]", columns, out var entry).ShouldBeTrue();
        entry.Name.ShouldBe("Name");
        entry.ValueType.ShouldBe(Db2ValueType.String);
        entry.ElementCount.ShouldBe(3);
        entry.IsNonInline.ShouldBeTrue();
        entry.IsRelation.ShouldBeTrue();
        entry.IsId.ShouldBeFalse();
    }

    [Fact]
    public void DbdLayoutEntryParser_TryParse_InlineType_MapsValueTypeAndCapturesToken()
    {
        DbdLayoutEntryParser.TryParse("SomeField<u32>", new Dictionary<string, DbdColumn>(), out var entry).ShouldBeTrue();
        entry.Name.ShouldBe("SomeField");
        entry.ValueType.ShouldBe(Db2ValueType.UInt64);
        entry.InlineTypeToken.ShouldBe("u32");
    }

    [Fact]
    public void DbdFile_Parse_ColumnsLayoutsBuilds_ParsesAndAttachesEntries()
    {
        var dbd = """
        // comment line
        COLUMNS
        int ID
        uint ParentID?
        string Name
        
        LAYOUT 2273DFFF, 60BB6C3F
        BUILD 1
        BUILD 2
        ID
        $noninline$ Name
        
        BUILD 3
        ParentID
        """;

        using var stream = new MemoryStream(Encoding.UTF8.GetBytes(dbd));
        var file = DbdFile.Parse(stream);

        file.ColumnsByName.Count.ShouldBe(3);
        file.Layouts.Count.ShouldBe(1);
        file.GlobalBuilds.Count.ShouldBe(0);

        var layout = file.Layouts[0];
        layout.Hashes.Length.ShouldBe(2);
        layout.Builds.Count.ShouldBe(3);

        layout.Builds[0].Entries.Count.ShouldBe(2);
        layout.Builds[1].Entries.Count.ShouldBe(2);
        layout.Builds[2].Entries.Count.ShouldBe(1);

        layout.Builds[0].GetPhysicalColumnCount().ShouldBe(1);
        layout.Builds[1].GetPhysicalColumnCount().ShouldBe(1);
        layout.Builds[2].GetPhysicalColumnCount().ShouldBe(1);
    }

    [Fact]
    public void DbdFile_TryGetLayout_MatchesOnHash()
    {
        using var stream = new MemoryStream(Encoding.UTF8.GetBytes("LAYOUT 2273DFFF\n"));
        var file = DbdFile.Parse(stream);
        file.TryGetLayout(0x2273DFFF, out var layout).ShouldBeTrue();
        layout.Hashes.ShouldBe([0x2273DFFF]);
    }

    [Fact]
    public void DbdLayout_TrySelectBuildByPhysicalColumnCount_FindsMatchingBuild()
    {
        var layout = new DbdLayout([1]);
        var b1 = new DbdBuildBlock("BUILD 1");
        b1.Entries.Add(new DbdLayoutEntry("A", Db2ValueType.Int64, null, 1, isVerified: true, isNonInline: false, isId: false, isRelation: false, inlineTypeToken: null));
        var b2 = new DbdBuildBlock("BUILD 2");
        b2.Entries.Add(new DbdLayoutEntry("A", Db2ValueType.Int64, null, 1, isVerified: true, isNonInline: false, isId: false, isRelation: false, inlineTypeToken: null));
        b2.Entries.Add(new DbdLayoutEntry("B", Db2ValueType.Int64, null, 1, isVerified: true, isNonInline: true, isId: false, isRelation: false, inlineTypeToken: null));
        layout.Builds.Add(b1);
        layout.Builds.Add(b2);

        layout.TrySelectBuildByPhysicalColumnCount(expected: 1, out var build, out var counts).ShouldBeTrue();
        build.BuildLine.ShouldBe("BUILD 1");
        counts.Length.ShouldBe(2);
        counts.ShouldBe([1, 0]);
    }
}
