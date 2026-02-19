namespace MimironSQL.Providers;

/// <summary>
/// Options controlling BLTE decoding behavior.
/// </summary>
/// <param name="OnSkippedBlock">Callback invoked when a block is skipped.</param>
/// <param name="TactKeyProvider">Optional provider for decrypting encrypted BLTE blocks.</param>
/// <param name="ThrowOnEncryptedBlockWithoutKey">
/// When <see langword="true"/>, throws when an encrypted BLTE block is encountered but cannot be decrypted.
/// When <see langword="false"/>, encrypted blocks are reported via <paramref name="OnSkippedBlock"/> and output is cleared/empty.
/// </param>
internal sealed record BlteDecodeOptions(
	Action<BlteSkippedBlock>? OnSkippedBlock = null,
	ITactKeyProvider? TactKeyProvider = null,
	bool ThrowOnEncryptedBlockWithoutKey = false);
