namespace MimironSQL.Providers;

public readonly record struct CascIdxHeaderSpec(
    byte Size,
    byte Offset,
    byte Key,
    byte OffsetBits);
