using System.Diagnostics.CodeAnalysis;

namespace MimironSQL.Providers;

[ExcludeFromCodeCoverage]
public readonly record struct CascStorageEncryptedBlteBlocksSkipped(CascKey EKey, int SkippedBlockCount, long SkippedLogicalBytes);
