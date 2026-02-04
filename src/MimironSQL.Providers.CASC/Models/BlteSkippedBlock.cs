namespace MimironSQL.Providers;

public readonly record struct BlteSkippedBlock(int BlockIndex, uint RawSize, uint LogicalSize, char Mode);
