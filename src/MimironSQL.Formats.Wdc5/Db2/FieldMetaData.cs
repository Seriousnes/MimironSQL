using System.Diagnostics.CodeAnalysis;

namespace MimironSQL.Formats.Wdc5.Db2;

[ExcludeFromCodeCoverage]
public readonly record struct FieldMetaData(short Bits, short Offset);
