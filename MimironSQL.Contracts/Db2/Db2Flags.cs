namespace MimironSQL.Db2;

[Flags]
public enum Db2Flags : ushort
{
    None = 0x0,
    Sparse = 0x1,
    SecondaryKey = 0x2,
    Index = 0x4,
    Unknown1 = 0x8,
    BitPacked = 0x10,
}
