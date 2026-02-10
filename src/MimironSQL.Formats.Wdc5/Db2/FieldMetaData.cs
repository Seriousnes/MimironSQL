using System.Diagnostics.CodeAnalysis;

namespace MimironSQL.Formats.Wdc5.Db2;

/// <summary>
/// Describes the bit width and offset for a logical field.
/// </summary>
/// <param name="Bits">Field bit-size metadata used by the DB2 spec.</param>
/// <param name="Offset">Field byte offset for sparse-mode records.</param>
[ExcludeFromCodeCoverage]
public readonly record struct FieldMetaData(short Bits, short Offset);
