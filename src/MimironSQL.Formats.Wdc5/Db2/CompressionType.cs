namespace MimironSQL.Formats.Wdc5.Db2;

public enum CompressionType : uint
{
    None = 0,
    Immediate = 1,
    Common = 2,
    Pallet = 3,
    PalletArray = 4,
    SignedImmediate = 5,
}
