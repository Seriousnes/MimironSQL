using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.Text;

namespace MimironSQL.DbContextGenerator.Tests.Helpers;

internal sealed class InMemoryAdditionalText(string path, SourceText text) : AdditionalText
{
    private readonly SourceText _text = text;

    public override string Path { get; } = path;

    public override SourceText GetText(CancellationToken cancellationToken = default) => _text;
}
