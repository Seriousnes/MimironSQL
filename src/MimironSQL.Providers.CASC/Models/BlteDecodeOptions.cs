namespace MimironSQL.Providers;

public sealed record BlteDecodeOptions(Action<BlteSkippedBlock>? OnSkippedBlock = null);
