using MimironSQL.Db2;
using MimironSQL.Db2.Schema;
using MimironSQL.Dbd;
using MimironSQL.Formats;
using MimironSQL.Formats.Wdc5;
using MimironSQL.Providers;

using Shouldly;

using System.Text;

namespace MimironSQL.Tests;

public sealed class SchemaMapperTests
{
    [Fact]
    public void Dbd_parser_supports_multiple_BUILD_lines_sharing_one_entry_block_and_noninline_fields_are_virtual()
    {
        var dbdText = """
            COLUMNS
            int ID
            int Type
            int Action
            int SlotID
            int ActionBar
            int ActionBarGroupID

            LAYOUT 40E3832D
            BUILD 11.0.0.53687
            BUILD 10.2.7.54171
            BUILD 10.2.6.53810
            $noninline,id$ID<32>
            Type<32>
            Action<32>
            SlotID<32>
            ActionBar<32>
            $noninline,relation$ActionBarGroupID<32>
            """;

        using var ms = new MemoryStream(Encoding.UTF8.GetBytes(dbdText));
        var parsed = DbdFile.Parse(ms);

        parsed.TryGetLayout(0x40E3832Du, out var layout).ShouldBeTrue();
        layout.Builds.Count.ShouldBe(3);

        foreach (var b in layout.Builds)
        {
            b.GetPhysicalColumnCount().ShouldBe(4);
            b.Entries.Count.ShouldBe(6);

            b.Entries[0].Name.ShouldBe("ID");
            b.Entries[0].IsNonInline.ShouldBeTrue();
            b.Entries[0].IsId.ShouldBeTrue();

            b.Entries[^1].Name.ShouldBe("ActionBarGroupID");
            b.Entries[^1].IsNonInline.ShouldBeTrue();
            b.Entries[^1].IsId.ShouldBeFalse();
        }
    }

    [Fact]
    public void Resolves_schema_from_layout_hash_and_matches_physical_column_count()
    {
        var db2Provider = new FileSystemDb2StreamProvider(new(TestDataPaths.GetTestDataDirectory()));
        using var db2Stream = db2Provider.OpenDb2Stream("Map");
        var file = new Wdc5File(db2Stream);

        var temp = Directory.CreateTempSubdirectory("MimironSQL_Dbd");
        try
        {
            var dbdText = CreateMapDbd(layoutHash: file.Header.LayoutHash, fieldsCount: file.Header.FieldsCount);
            File.WriteAllText(Path.Combine(temp.FullName, "Map.dbd"), dbdText, Encoding.UTF8);

            var provider = new FileSystemDbdProvider(new FileSystemDbdProviderOptions(temp.FullName));
            var mapper = new SchemaMapper(provider);
            var schema = mapper.GetSchema("Map", new Db2FileLayout(file.Header.LayoutHash, file.Header.FieldsCount));

            schema.LayoutHash.ShouldBe(file.Header.LayoutHash);
            schema.PhysicalColumnCount.ShouldBe(file.Header.FieldsCount);

            schema.TryGetField("ID", out var id).ShouldBeTrue();
            id.IsVirtual.ShouldBeTrue();
            id.IsId.ShouldBeTrue();
            id.IsVerified.ShouldBeTrue();
            id.ElementCount.ShouldBe(0);
            id.ColumnStartIndex.ShouldBe(-1);
            id.IsRelation.ShouldBeFalse();

            schema.TryGetField("Directory", out var directory).ShouldBeTrue();
            directory.IsVirtual.ShouldBeFalse();
            directory.IsVerified.ShouldBeTrue();
            directory.ValueType.ShouldBe(Db2ValueType.String);
            directory.ColumnStartIndex.ShouldBe(0);
            directory.ElementCount.ShouldBe(1);
            directory.IsRelation.ShouldBeFalse();

            schema.TryGetField("MinimapIconScale", out var scale).ShouldBeTrue();
            scale.ValueType.ShouldBe(Db2ValueType.Single);
            scale.ColumnStartIndex.ShouldBe(1);
            scale.ElementCount.ShouldBe(1);
            scale.IsVerified.ShouldBeTrue();

            schema.TryGetField("Flags", out var flags).ShouldBeTrue();
            flags.ValueType.ShouldBe(Db2ValueType.Int64);
            flags.ColumnStartIndex.ShouldBe(2);
            flags.ElementCount.ShouldBe(2);
            flags.IsVerified.ShouldBeTrue();
            flags.IsRelation.ShouldBeFalse();
        }
        finally
        {
            temp.Delete(recursive: true);
        }
    }

    [Fact]
    public void Resolves_relation_and_reference_table_from_dbd()
    {
        var db2Provider = new FileSystemDb2StreamProvider(new(TestDataPaths.GetTestDataDirectory()));
        using var stream = db2Provider.OpenDb2Stream("CollectableSourceQuestSparse");
        var file = new Wdc5File(stream);

        var provider = new FileSystemDbdProvider(new FileSystemDbdProviderOptions(TestDataPaths.GetTestDataDirectory()));
        var mapper = new SchemaMapper(provider);
        var schema = mapper.GetSchema("CollectableSourceQuestSparse", new Db2FileLayout(file.Header.LayoutHash, file.Header.FieldsCount));

        schema.TryGetField("QuestID", out var questId).ShouldBeTrue();
        questId.IsRelation.ShouldBeFalse();
        questId.ReferencedTableName.ShouldBe("QuestV2");
        questId.IsVerified.ShouldBeFalse();

        schema.TryGetField("CollectableSourceInfoID", out var sourceInfo).ShouldBeTrue();
        sourceInfo.IsVirtual.ShouldBeTrue();
        sourceInfo.IsRelation.ShouldBeTrue();
        sourceInfo.ReferencedTableName.ShouldBe("CollectableSourceInfo");
        sourceInfo.IsVerified.ShouldBeFalse();
    }

    [Fact]
    public void AccountStoreCategory_real_dbd_db2_inline_id_and_relation_metadata()
    {
        var db2Provider = new FileSystemDb2StreamProvider(new(TestDataPaths.GetTestDataDirectory()));
        using var stream = db2Provider.OpenDb2Stream("AccountStoreCategory");
        var file = new Wdc5File(stream);

        var provider = new FileSystemDbdProvider(new FileSystemDbdProviderOptions(TestDataPaths.GetTestDataDirectory()));
        var mapper = new SchemaMapper(provider);
        var schema = mapper.GetSchema("AccountStoreCategory", new Db2FileLayout(file.Header.LayoutHash, file.Header.FieldsCount));

        schema.LayoutHash.ShouldBe(file.Header.LayoutHash);
        schema.PhysicalColumnCount.ShouldBe(file.Header.FieldsCount);

        schema.TryGetField("ID", out var id).ShouldBeTrue();
        id.IsVirtual.ShouldBeFalse();
        id.IsId.ShouldBeTrue();
        id.IsVerified.ShouldBeFalse();

        schema.TryGetField("StoreFrontID", out var storeFrontId).ShouldBeTrue();
        storeFrontId.IsVirtual.ShouldBeFalse();
        storeFrontId.IsRelation.ShouldBeTrue();
        storeFrontId.IsVerified.ShouldBeFalse();
    }

    [Fact]
    public void Selects_the_build_block_with_matching_physical_column_count()
    {
        var db2Provider = new FileSystemDb2StreamProvider(new(TestDataPaths.GetTestDataDirectory()));
        using var db2Stream = db2Provider.OpenDb2Stream("Map");
        var file = new Wdc5File(db2Stream);

        var temp = Directory.CreateTempSubdirectory("MimironSQL_Dbd");
        try
        {
            var dbdText = CreateMapDbdWithWrongAndRightBuilds(layoutHash: file.Header.LayoutHash, fieldsCount: file.Header.FieldsCount);
            File.WriteAllText(Path.Combine(temp.FullName, "Map.dbd"), dbdText, Encoding.UTF8);

            var provider = new FileSystemDbdProvider(new FileSystemDbdProviderOptions(temp.FullName));
            var mapper = new SchemaMapper(provider);
            var schema = mapper.GetSchema("Map", new Db2FileLayout(file.Header.LayoutHash, file.Header.FieldsCount));

            schema.PhysicalColumnCount.ShouldBe(file.Header.FieldsCount);
            schema.TryGetField("LastField", out var last).ShouldBeTrue();
            last.ColumnStartIndex.ShouldBe(file.Header.FieldsCount - 1);
        }
        finally
        {
            temp.Delete(recursive: true);
        }
    }

    [Fact]
    public void Trailing_question_mark_in_COLUMNS_marks_field_as_unverified()
    {
        var db2Provider = new FileSystemDb2StreamProvider(new(TestDataPaths.GetTestDataDirectory()));
        using var db2Stream = db2Provider.OpenDb2Stream("Map");
        var file = new Wdc5File(db2Stream);

        var temp = Directory.CreateTempSubdirectory("MimironSQL_Dbd");
        try
        {
            var dbdText = CreateDbdWithUnverifiedColumn(layoutHash: file.Header.LayoutHash, fieldsCount: file.Header.FieldsCount);
            File.WriteAllText(Path.Combine(temp.FullName, "Map.dbd"), dbdText, Encoding.UTF8);

            var provider = new FileSystemDbdProvider(new FileSystemDbdProviderOptions(temp.FullName));
            var mapper = new SchemaMapper(provider);
            var schema = mapper.GetSchema("Map", new Db2FileLayout(file.Header.LayoutHash, file.Header.FieldsCount));

            schema.TryGetField("Unverified", out var unverified).ShouldBeTrue();
            unverified.IsVerified.ShouldBeFalse();
            unverified.IsVirtual.ShouldBeFalse();
            unverified.ValueType.ShouldBe(Db2ValueType.Int64);
            unverified.ColumnStartIndex.ShouldBe(0);
            unverified.ElementCount.ShouldBe(1);
        }
        finally
        {
            temp.Delete(recursive: true);
        }
    }

    private static string CreateMapDbd(uint layoutHash, int fieldsCount)
    {
        // Physical columns:
        // Directory (1) + MinimapIconScale (1) + Flags[2] (1 physical field) + fillers (fieldsCount - 3)
        var fillersNeeded = fieldsCount - 3;

        var sb = new StringBuilder();
        sb.AppendLine("COLUMNS");
        sb.AppendLine("int ID");
        sb.AppendLine("string Directory");
        sb.AppendLine("float MinimapIconScale");
        sb.AppendLine("int Flags");
        sb.AppendLine();

        sb.AppendLine($"LAYOUT {layoutHash:X8}");
        sb.AppendLine("BUILD test");
        sb.AppendLine("$noninline,id$ID<32>");
        sb.AppendLine("Directory");
        sb.AppendLine("MinimapIconScale");
        sb.AppendLine("Flags<32>[2]");

        for (var i = 0; i < fillersNeeded; i++)
            sb.AppendLine($"Field_{i}<32>");

        return sb.ToString();
    }

    private static string CreateMapDbdWithWrongAndRightBuilds(uint layoutHash, int fieldsCount)
    {
        var sb = new StringBuilder();
        sb.AppendLine("COLUMNS");
        sb.AppendLine("int ID");
        sb.AppendLine();

        sb.AppendLine($"LAYOUT {layoutHash:X8}");

        // Wrong build: physical columns are short by 1.
        sb.AppendLine("BUILD wrong");
        sb.AppendLine("$noninline,id$ID<32>");
        for (var i = 0; i < Math.Max(0, fieldsCount - 1); i++)
            sb.AppendLine($"Field_{i}<32>");

        sb.AppendLine();

        // Right build: matches FieldsCount exactly and ends with LastField.
        sb.AppendLine("BUILD right");
        sb.AppendLine("$noninline,id$ID<32>");
        for (var i = 0; i < Math.Max(0, fieldsCount - 1); i++)
            sb.AppendLine($"Field_{i}<32>");
        sb.AppendLine("LastField<32>");

        return sb.ToString();
    }

    private static string CreateDbdWithUnverifiedColumn(uint layoutHash, int fieldsCount)
    {
        var fillersNeeded = fieldsCount - 1;

        var sb = new StringBuilder();
        sb.AppendLine("COLUMNS");
        sb.AppendLine("int ID");
        sb.AppendLine("int Unverified?");
        sb.AppendLine();

        sb.AppendLine($"LAYOUT {layoutHash:X8}");
        sb.AppendLine("BUILD test");
        sb.AppendLine("$noninline,id$ID<32>");
        sb.AppendLine("Unverified<32>");

        for (var i = 0; i < fillersNeeded; i++)
            sb.AppendLine($"Field_{i}<32>");

        return sb.ToString();
    }
}
