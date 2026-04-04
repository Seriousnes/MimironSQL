using Shouldly;

using MimironSQL.Formats.Wdc5.Db2;

namespace MimironSQL.Formats.Wdc5.Tests;

public sealed class Wdc5FieldAccessorTests(Wdc5TestFixture fixture) : IClassFixture<Wdc5TestFixture>
{
    private readonly Wdc5TestFixture _fixture = fixture;

    [Fact]
    public void CreateFieldAccessor_DenseImmediateScalar_MatchesReadField_AndMaterializesLazyRecords()
    {
        using var stream = _fixture.CreateSingleSectionDenseImmediateScalarFile(rowId: 42, value: 0x01020304);
        var file = new Wdc5File(stream, new Wdc5FileOptions(RecordLoadingMode: Wdc5RecordLoadingMode.Lazy));

        file.ParsedSections[0].RecordsData.ShouldBeNull();
        file.TryGetRowById(42, out var row).ShouldBeTrue();

        var accessor = file.CreateFieldAccessor(sectionIndex: 0, fieldIndex: 0);

        accessor.ReadScalar<int>(row.RowIndexInSection).ShouldBe(file.ReadField<int>(row, 0));
        accessor.ReadScalarRaw(row.RowIndexInSection).ShouldBe(0x01020304u);
        file.ParsedSections[0].RecordsData.ShouldNotBeNull();
    }

    [Fact]
    public void CreateFieldAccessor_SparseFile_Throws()
    {
        using var stream = _fixture.CreateSingleSectionSparseMultiFieldInlineStringFile(rowId: 99, field0Value: 1234, field1StringBytes: [(byte)'h', (byte)'i', 0]);
        var file = new Wdc5File(stream);

        Should.Throw<NotSupportedException>(() => file.CreateFieldAccessor(sectionIndex: 0, fieldIndex: 0))
            .Message.ShouldContain("CreateSparseFieldAccessor");
    }

    [Fact]
    public void CreateSparseFieldAccessor_SparseImmediateScalar_MatchesReadField()
    {
        using var stream = _fixture.CreateSingleSectionSparseMultiFieldInlineStringFile(rowId: 77, field0Value: 0x0A0B0C0D, field1StringBytes: [(byte)'o', (byte)'k', 0]);
        var file = new Wdc5File(stream, new Wdc5FileOptions(RecordLoadingMode: Wdc5RecordLoadingMode.Lazy));
        file.TryGetRowById(77, out var row).ShouldBeTrue();

        var accessor = file.CreateSparseFieldAccessor(sectionIndex: 0, fieldIndex: 0);

        accessor.ReadScalar<int>(row.RowIndexInSection).ShouldBe(file.ReadField<int>(row, 0));
        accessor.ReadScalarRaw(row.RowIndexInSection).ShouldBe(0x0A0B0C0Du);
    }

    [Fact]
    public void SparseOffsetTable_IsLazyByDefault_AndBuiltOnFirstSparseStringAccess()
    {
        using var stream = _fixture.CreateSingleSectionSparseScalar16ThenInlineStringFile(rowId: 201, field0: 0xABCD, field1StringBytes: [(byte)'h', (byte)'i', 0]);
        var file = new Wdc5File(stream);

        file.ParsedSections[0].SparseOffsetTable.ShouldNotBeNull();
        file.ParsedSections[0].SparseOffsetTable!.IsValueCreated.ShouldBeFalse();

        file.TryGetRowById(201, out var row).ShouldBeTrue();
        file.ReadField<string>(row, 1).ShouldBe("hi");

        file.ParsedSections[0].SparseOffsetTable!.IsValueCreated.ShouldBeTrue();
    }

    [Fact]
    public void SparseOffsetTable_CanBeBuiltEagerly()
    {
        using var stream = _fixture.CreateSingleSectionSparseScalar16ThenInlineStringFile(rowId: 202, field0: 0x1234, field1StringBytes: [(byte)'y', (byte)'e', (byte)'s', 0]);
        var file = new Wdc5File(stream, new Wdc5FileOptions(EagerSparseOffsetTable: true));

        file.ParsedSections[0].SparseOffsetTable.ShouldNotBeNull();
        file.ParsedSections[0].SparseOffsetTable!.IsValueCreated.ShouldBeTrue();
    }
}
