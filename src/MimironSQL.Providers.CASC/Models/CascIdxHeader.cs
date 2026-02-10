namespace MimironSQL.Providers;

/// <summary>
/// Represents the header of a CASC <c>.idx</c> file.
/// </summary>
internal sealed class CascIdxHeader
{
    /// <summary>
    /// Gets the size of the header hash field.
    /// </summary>
    public int HeaderHashSize { get; init; }

    /// <summary>
    /// Gets the header hash value.
    /// </summary>
    public uint HeaderHash { get; init; }

    /// <summary>
    /// Gets the header version.
    /// </summary>
    public ushort Version { get; init; }

    /// <summary>
    /// Gets the bucket index this <c>.idx</c> file belongs to.
    /// </summary>
    public byte BucketIndex { get; init; }

    /// <summary>
    /// Gets the extra bytes value from the header.
    /// </summary>
    public byte ExtraBytes { get; init; }

    /// <summary>
    /// Gets the record layout specification for entries in this <c>.idx</c> file.
    /// </summary>
    public CascIdxHeaderSpec Spec { get; init; }

    /// <summary>
    /// Gets the maximum total archive size declared by the header.
    /// </summary>
    public ulong ArchiveTotalSizeMaximum { get; init; }

    /// <summary>
    /// Gets the size of the entries data block.
    /// </summary>
    public uint EntriesSize { get; init; }

    /// <summary>
    /// Gets the entries hash value.
    /// </summary>
    public uint EntriesHash { get; init; }

    /// <summary>
    /// Gets the size, in bytes, of a single entry record.
    /// </summary>
    public int RecordSize => Spec.Key + Spec.Offset + Spec.Size;
}
