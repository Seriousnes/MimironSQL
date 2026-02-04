namespace MimironSQL.Providers;

public sealed class CascIdxHeader
{
    public int HeaderHashSize { get; init; }
    public uint HeaderHash { get; init; }
    public ushort Version { get; init; }
    public byte BucketIndex { get; init; }
    public byte ExtraBytes { get; init; }

    public CascIdxHeaderSpec Spec { get; init; }

    public ulong ArchiveTotalSizeMaximum { get; init; }
    public uint EntriesSize { get; init; }
    public uint EntriesHash { get; init; }

    public int RecordSize => Spec.Key + Spec.Offset + Spec.Size;
}
