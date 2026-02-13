using Microsoft.EntityFrameworkCore.Query;

namespace MimironSQL.EntityFrameworkCore.Db2.Query;

/// <summary>
/// Factory that creates <see cref="Db2QueryTranslationPreprocessor"/> instances.
/// </summary>
internal sealed class Db2QueryTranslationPreprocessorFactory(
    QueryTranslationPreprocessorDependencies dependencies)
    : IQueryTranslationPreprocessorFactory
{
    private readonly QueryTranslationPreprocessorDependencies _dependencies = dependencies;

    public QueryTranslationPreprocessor Create(QueryCompilationContext queryCompilationContext)
        => new Db2QueryTranslationPreprocessor(_dependencies, queryCompilationContext);
}
