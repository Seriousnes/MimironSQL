using System.Diagnostics.CodeAnalysis;
using System.Runtime.InteropServices;

namespace MimironSQL.Formats.Wdc5.Db2;

/// <summary>
/// Describes per-column metadata for a WDC5 section.
/// </summary>
[ExcludeFromCodeCoverage]
[StructLayout(LayoutKind.Explicit, Pack = 1, Size = 24)]
public struct ColumnMetaData
{
    /// <summary>
    /// Bit offset of the field within a record.
    /// </summary>
    [FieldOffset(0)]
    public ushort RecordOffset;

    /// <summary>
    /// Field size in bits.
    /// </summary>
    [FieldOffset(2)]
    public ushort Size;

    /// <summary>
    /// Additional data size in bytes (e.g. pallet/common data).
    /// </summary>
    [FieldOffset(4)]
    public uint AdditionalDataSize;

    /// <summary>
    /// Compression mode used for the field.
    /// </summary>
    [FieldOffset(8)]
    public CompressionType CompressionType;

    /// <summary>
    /// Compression parameters for <see cref="Db2.CompressionType.Immediate"/> and related modes.
    /// </summary>
    [FieldOffset(12)]
    public ColumnCompressionDataImmediate Immediate;

    /// <summary>
    /// Compression parameters for <see cref="Db2.CompressionType.Pallet"/> and <see cref="Db2.CompressionType.PalletArray"/>.
    /// </summary>
    [FieldOffset(12)]
    public ColumnCompressionDataPallet Pallet;

    /// <summary>
    /// Compression parameters for <see cref="Db2.CompressionType.Common"/>.
    /// </summary>
    [FieldOffset(12)]
    public ColumnCompressionDataCommon Common;
}

/// <summary>
/// Parameters for immediate compression.
/// </summary>
/// <param name="BitOffset">Bit offset within the record for packed reads.</param>
/// <param name="BitWidth">Number of bits used to encode the value.</param>
/// <param name="Flags">Format-specific flags.</param>
[ExcludeFromCodeCoverage]
public readonly record struct ColumnCompressionDataImmediate(int BitOffset, int BitWidth, int Flags);

/// <summary>
/// Parameters for pallet-based compression.
/// </summary>
/// <param name="BitOffset">Bit offset within the record for packed reads.</param>
/// <param name="BitWidth">Number of bits used for the pallet index.</param>
/// <param name="Cardinality">Number of values per row for pallet-array fields.</param>
[ExcludeFromCodeCoverage]
public readonly record struct ColumnCompressionDataPallet(int BitOffset, int BitWidth, int Cardinality);

/// <summary>
/// Parameters for common-value compression.
/// </summary>
/// <param name="DefaultValue">Default value used when no entry exists for a row ID.</param>
/// <param name="B">Format-specific parameter.</param>
/// <param name="C">Format-specific parameter.</param>
[ExcludeFromCodeCoverage]
public readonly record struct ColumnCompressionDataCommon(uint DefaultValue, int B, int C);
