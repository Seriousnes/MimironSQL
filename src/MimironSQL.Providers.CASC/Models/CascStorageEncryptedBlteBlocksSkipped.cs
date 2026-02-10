using System.Diagnostics.CodeAnalysis;

namespace MimironSQL.Providers;

/// <summary>
/// Contains information about encrypted BLTE blocks that were skipped while reading from CASC storage.
/// </summary>
/// <param name="EKey">The encoded key that was being read.</param>
/// <param name="SkippedBlockCount">The number of skipped blocks.</param>
/// <param name="SkippedLogicalBytes">The total logical bytes skipped.</param>
[ExcludeFromCodeCoverage]
internal readonly record struct CascStorageEncryptedBlteBlocksSkipped(CascKey EKey, int SkippedBlockCount, long SkippedLogicalBytes);
