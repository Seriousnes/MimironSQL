namespace MimironSQL.Providers;

public sealed record ListfileRecord
{
    public int FileDataId { get; init; }

    public string FileName { get; init; } = string.Empty;
}
