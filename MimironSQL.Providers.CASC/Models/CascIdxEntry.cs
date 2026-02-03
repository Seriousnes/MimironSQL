namespace MimironSQL.Providers;

public readonly record struct CascIdxEntry(ReadOnlyMemory<byte> KeyPrefix, int ArchiveIndex, long Offset, uint Size);
