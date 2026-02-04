namespace MimironSQL.Providers;

public readonly record struct CascStorageEncryptedBlteBlocksSkipped(CascKey EKey, int SkippedBlockCount, long SkippedLogicalBytes);
