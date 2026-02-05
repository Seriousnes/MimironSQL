using System.Text;

using Shouldly;

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

    [Fact]
    public void DbdBuildBlock_GetPhysicalColumnCount_SkipsNonInlineEntries()
    {
        var build = new DbdBuildBlock("BUILD 1");
        build.Entries.Add(new DbdLayoutEntry("A", Db2ValueType.Int64, null, 1, isVerified: true, isNonInline: false, isId: false, isRelation: false, inlineTypeToken: null));
        build.Entries.Add(new DbdLayoutEntry("B", Db2ValueType.Int64, null, 1, isVerified: true, isNonInline: true, isId: false, isRelation: false, inlineTypeToken: null));
        build.GetPhysicalColumnCount().ShouldBe(1);

        ((IDbdBuildBlock)build).Entries.Count.ShouldBe(2);
    }

    [Fact]
    public void DbdColumnParser_TryParse_InvalidInput_ReturnsFalse()
    {
        DbdColumnParser.TryParse("int", out var name1, out var col1).ShouldBeFalse();
        name1.ShouldBe(string.Empty);
        col1.ShouldBeNull();

        DbdColumnParser.TryParse("int ", out var name2, out var col2).ShouldBeFalse();
        name2.ShouldBe(string.Empty);
        col2.ShouldBeNull();

        DbdColumnParser.TryParse("int   ", out var name3, out var col3).ShouldBeFalse();
        name3.ShouldBe(string.Empty);
        col3.ShouldBeNull();
    }

    [Fact]
    public void DbdColumnParser_TryParse_MapsLocStringAndUnknownTypeTokens()
    {
        DbdColumnParser.TryParse("locstring Title", out var name1, out var col1).ShouldBeTrue();
        name1.ShouldBe("Title");
        col1.ValueType.ShouldBe(Db2ValueType.LocString);
        col1.IsVerified.ShouldBeTrue();

        DbdColumnParser.TryParse("float F", out var nameFloat, out var colFloat).ShouldBeTrue();
        nameFloat.ShouldBe("F");
        colFloat.ValueType.ShouldBe(Db2ValueType.Single);

        DbdColumnParser.TryParse("byte Foo", out var name2, out var col2).ShouldBeTrue();
        name2.ShouldBe("Foo");
        col2.ValueType.ShouldBe(Db2ValueType.Unknown);
    }

    [Fact]
    public void DbdColumnParser_TryParse_TypeTokenReferenceEdgeCases()
    {
        DbdColumnParser.TryParse("int<::ID> Foo", out var name1, out var col1).ShouldBeTrue();
        name1.ShouldBe("Foo");
        col1.ValueType.ShouldBe(Db2ValueType.Int64);
        col1.ReferencedTableName.ShouldBeNull();

        DbdColumnParser.TryParse("int<Map::ID> Bar", out var name2, out var col2).ShouldBeTrue();
        name2.ShouldBe("Bar");
        col2.ReferencedTableName.ShouldBe("Map");

        DbdColumnParser.TryParse("int<Map::ID Bar", out var name3, out var col3).ShouldBeTrue();
        name3.ShouldBe("Bar");
        col3.ValueType.ShouldBe(Db2ValueType.Int64);
        col3.ReferencedTableName.ShouldBeNull();
    }

    [Fact]
    public void DbdFile_Parse_SplitsGlobalBuildsAndLayoutBuilds_AndStripsInlineComments()
    {
        var dbd = """
        COLUMNS
        int ID // inline comment

        BUILD 100
        ID

        LAYOUT 2273DFFF
        BUILD 1
        COMMENT ignored
        ID
        """;

        using var stream = new MemoryStream(Encoding.UTF8.GetBytes(dbd));
        var file = DbdFile.Parse(stream);

        file.ColumnsByName.ContainsKey("ID").ShouldBeTrue();
        file.GlobalBuilds.Count.ShouldBe(1);
        file.Layouts.Count.ShouldBe(1);
        file.Layouts[0].Builds.Count.ShouldBe(1);

        file.GlobalBuilds[0].Entries.Count.ShouldBe(1);
        file.Layouts[0].Builds[0].Entries.Count.ShouldBe(1);

        ((IDbdFile)file).ColumnsByName.ContainsKey("ID").ShouldBeTrue();
        ((IDbdFile)file).Layouts.Count.ShouldBe(1);
        ((IDbdFile)file).GlobalBuilds.Count.ShouldBe(1);
    }

    [Fact]
    public void DbdFile_Parse_BuildsShareEntriesUntilNewBuildAfterEntries()
    {
        var dbd = """
        BUILD 1
        BUILD 2
        A
        B

        BUILD 3
        C
        """;

        using var stream = new MemoryStream(Encoding.UTF8.GetBytes(dbd));
        var file = DbdFile.Parse(stream);

        file.Layouts.Count.ShouldBe(0);
        file.GlobalBuilds.Count.ShouldBe(3);

        file.GlobalBuilds[0].Entries.Count.ShouldBe(2);
        file.GlobalBuilds[1].Entries.Count.ShouldBe(2);
        file.GlobalBuilds[2].Entries.Count.ShouldBe(1);

        file.GlobalBuilds[0].Entries[0].Name.ShouldBe("A");
        file.GlobalBuilds[0].Entries[1].Name.ShouldBe("B");
        file.GlobalBuilds[2].Entries[0].Name.ShouldBe("C");
    }

    [Fact]
    public void DbdFile_TryGetLayout_NotFound_ReturnsFalse_ForConcreteAndInterface()
    {
        using var stream = new MemoryStream(Encoding.UTF8.GetBytes("LAYOUT 11111111\n"));
        var file = DbdFile.Parse(stream);

        file.TryGetLayout(0x22222222, out _).ShouldBeFalse();
        ((IDbdFile)file).TryGetLayout(0x22222222, out _).ShouldBeFalse();
    }

    [Fact]
    public void DbdFile_TryGetLayout_Found_ReturnsTrue_ForInterface()
    {
        using var stream = new MemoryStream(Encoding.UTF8.GetBytes("LAYOUT 2273DFFF\n"));
        var file = DbdFile.Parse(stream);

        ((IDbdFile)file).TryGetLayout(0x2273DFFF, out var layout).ShouldBeTrue();
        layout.Hashes.Count.ShouldBe(1);
        layout.Hashes[0].ShouldBe(0x2273DFFFu);
    }

    [Fact]
    public void DbdLayout_ContainsHash_False_WhenMissing()
    {
        new DbdLayout([1]).ContainsHash(2).ShouldBeFalse();
    }

    [Fact]
    public void DbdLayout_TrySelectBuildByPhysicalColumnCount_NoMatch_ReturnsFalseAndAllCounts()
    {
        var layout = new DbdLayout([1]);
        var b1 = new DbdBuildBlock("BUILD 1");
        b1.Entries.Add(new DbdLayoutEntry("A", Db2ValueType.Int64, null, 1, isVerified: true, isNonInline: false, isId: false, isRelation: false, inlineTypeToken: null));
        var b2 = new DbdBuildBlock("BUILD 2");
        b2.Entries.Add(new DbdLayoutEntry("A", Db2ValueType.Int64, null, 1, isVerified: true, isNonInline: false, isId: false, isRelation: false, inlineTypeToken: null));
        b2.Entries.Add(new DbdLayoutEntry("B", Db2ValueType.Int64, null, 1, isVerified: true, isNonInline: false, isId: false, isRelation: false, inlineTypeToken: null));
        layout.Builds.Add(b1);
        layout.Builds.Add(b2);

        layout.TrySelectBuildByPhysicalColumnCount(expected: 3, out _, out var counts).ShouldBeFalse();
        counts.ShouldBe([1, 2]);

        ((IDbdLayout)layout).TrySelectBuildByPhysicalColumnCount(expected: 3, out _, out var interfaceCounts).ShouldBeFalse();
        interfaceCounts.ShouldBe([1, 2]);
    }

    [Fact]
    public void DbdLayout_InterfaceMembers_AreAccessible_AndMatchConcrete()
    {
        var layout = new DbdLayout([1, 2]);
        var b = new DbdBuildBlock("BUILD 1");
        b.Entries.Add(new DbdLayoutEntry("A", Db2ValueType.Int64, null, 1, isVerified: true, isNonInline: false, isId: false, isRelation: false, inlineTypeToken: null));
        layout.Builds.Add(b);

        var i = (IDbdLayout)layout;
        i.Hashes.Count.ShouldBe(2);
        i.Builds.Count.ShouldBe(1);

        i.TrySelectBuildByPhysicalColumnCount(expected: 1, out var found, out var availableCounts).ShouldBeTrue();
        found.BuildLine.ShouldBe("BUILD 1");
        availableCounts.Length.ShouldBe(1);
        availableCounts[0].ShouldBe(1);
    }

    [Fact]
    public void DbdLayoutEntryParser_TryParse_ReturnsFalseForEmptyAndMalformedInputs()
    {
        DbdLayoutEntryParser.TryParse("   ", new Dictionary<string, DbdColumn>(), out _).ShouldBeFalse();
        DbdLayoutEntryParser.TryParse("Field<u32", new Dictionary<string, DbdColumn>(), out _).ShouldBeFalse();
        DbdLayoutEntryParser.TryParse("<u32>", new Dictionary<string, DbdColumn>(), out _).ShouldBeFalse();

        DbdLayoutEntryParser.TryParse("$noninline, relation$ Field", new Dictionary<string, DbdColumn>(), out _).ShouldBeFalse();
    }

    [Fact]
    public void DbdLayoutEntryParser_TryParse_ParsesModifierTokens_IgnoresEmptyTokens()
    {
        var columns = new Dictionary<string, DbdColumn>(StringComparer.Ordinal)
        {
            ["Field"] = new DbdColumn(Db2ValueType.Int64, referencedTableName: null, isVerified: true),
        };

        DbdLayoutEntryParser.TryParse("$,id,,relation,$ Field", columns, out var entry).ShouldBeTrue();
        entry.IsId.ShouldBeTrue();
        entry.IsRelation.ShouldBeTrue();
        entry.IsNonInline.ShouldBeFalse();
    }

    [Fact]
    public void DbdLayoutEntryParser_TryParse_InlineType_MapsMultipleTypeShapes()
    {
        DbdLayoutEntryParser.TryParse("A<u16>", new Dictionary<string, DbdColumn>(), out var u).ShouldBeTrue();
        u.ValueType.ShouldBe(Db2ValueType.UInt64);
        u.InlineTypeToken.ShouldBe("u16");

        DbdLayoutEntryParser.TryParse("B<f32>", new Dictionary<string, DbdColumn>(), out var f).ShouldBeTrue();
        f.ValueType.ShouldBe(Db2ValueType.Single);

        DbdLayoutEntryParser.TryParse("C<16>", new Dictionary<string, DbdColumn>(), out var i).ShouldBeTrue();
        i.ValueType.ShouldBe(Db2ValueType.Int64);

        DbdLayoutEntryParser.TryParse("D<weird>", new Dictionary<string, DbdColumn>(), out var d).ShouldBeTrue();
        d.ValueType.ShouldBe(Db2ValueType.Int64);
    }

    [Fact]
    public void DbdLayoutEntryParser_TryParse_InlineType_PicksUpReferenceFromColumnsByName()
    {
        var columns = new Dictionary<string, DbdColumn>(StringComparer.Ordinal)
        {
            ["ParentMapID"] = new DbdColumn(Db2ValueType.Int64, referencedTableName: "Map", isVerified: false),
        };

        DbdLayoutEntryParser.TryParse("ParentMapID<u32>", columns, out var entry).ShouldBeTrue();
        entry.ReferencedTableName.ShouldBe("Map");
        entry.IsVerified.ShouldBeFalse();
        entry.ValueType.ShouldBe(Db2ValueType.UInt64);
        entry.InlineTypeToken.ShouldBe("u32");
    }

    [Fact]
    public void DbdLayoutEntryParser_TryParse_DoesNotTreatUnclosedModifiersOrBadArraysAsModifiersOrArrays()
    {
        DbdLayoutEntryParser.TryParse("$$ Name", new Dictionary<string, DbdColumn>(), out var e1).ShouldBeTrue();
        e1.Name.ShouldBe("$$ Name");

        DbdLayoutEntryParser.TryParse("$noninline Name", new Dictionary<string, DbdColumn>(), out var e2).ShouldBeTrue();
        e2.Name.ShouldBe("$noninline Name");

        DbdLayoutEntryParser.TryParse("Name[3", new Dictionary<string, DbdColumn>(), out var e3).ShouldBeTrue();
        e3.ElementCount.ShouldBe(1);
        e3.Name.ShouldBe("Name[3");
    }
}
