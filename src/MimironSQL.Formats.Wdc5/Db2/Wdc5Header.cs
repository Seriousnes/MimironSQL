using System.Diagnostics.CodeAnalysis;

using MimironSQL.Db2;
using MimironSQL.Formats;

namespace MimironSQL.Formats.Wdc5.Db2;

/// <summary>
/// Represents the parsed WDC5 file header.
/// </summary>
/// <param name="SchemaVersion">Schema version value from the file header.</param>
/// <param name="SchemaString">Schema string from the file header (typically null-terminated).</param>
/// <param name="RecordsCount">Number of records in the file.</param>
/// <param name="FieldsCount">Number of logical fields in each record.</param>
/// <param name="RecordSize">Size of each record in bytes.</param>
/// <param name="StringTableSize">Size of the string table in bytes.</param>
/// <param name="TableHash">Table hash value from the header.</param>
/// <param name="LayoutHash">Layout hash value from the header.</param>
/// <param name="MinIndex">Minimum record index value from the header.</param>
/// <param name="MaxIndex">Maximum record index value from the header.</param>
/// <param name="Locale">Locale value from the header.</param>
/// <param name="Flags">DB2 flags.</param>
/// <param name="IdFieldIndex">Zero-based index of the ID field.</param>
/// <param name="TotalFieldsCount">Total fields count (including hidden fields) from the header.</param>
/// <param name="PackedDataOffset">Packed data offset from the header.</param>
/// <param name="LookupColumnCount">Lookup column count from the header.</param>
/// <param name="ColumnMetaDataSize">Column metadata size in bytes.</param>
/// <param name="CommonDataSize">Common data size in bytes.</param>
/// <param name="PalletDataSize">Pallet data size in bytes.</param>
/// <param name="SectionsCount">Number of sections in the file.</param>
[ExcludeFromCodeCoverage]
public readonly record struct Wdc5Header(
    uint SchemaVersion,
    string SchemaString,
    int RecordsCount,
    int FieldsCount,
    int RecordSize,
    int StringTableSize,
    uint TableHash,
    uint LayoutHash,
    int MinIndex,
    int MaxIndex,
    int Locale,
    Db2Flags Flags,
    ushort IdFieldIndex,
    int TotalFieldsCount,
    int PackedDataOffset,
    int LookupColumnCount,
    int ColumnMetaDataSize,
    int CommonDataSize,
    int PalletDataSize,
    int SectionsCount) : IDb2FileHeader;
