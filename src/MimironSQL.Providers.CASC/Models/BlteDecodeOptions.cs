namespace MimironSQL.Providers;

/// <summary>
/// Options controlling BLTE decoding behavior.
/// </summary>
/// <param name="OnSkippedBlock">Callback invoked when a block is skipped.</param>
internal sealed record BlteDecodeOptions(Action<BlteSkippedBlock>? OnSkippedBlock = null);
