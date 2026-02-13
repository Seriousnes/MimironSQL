namespace MimironSQL.EntityFrameworkCore.Db2.Query.Expressions;

/// <summary>
/// Sentinel value stored in filter expressions when the actual value is a
/// runtime parameter that must be resolved from <c>QueryContext.ParameterValues</c>.
/// </summary>
/// <param name="Name">The parameter name (e.g., <c>__expectedId_0</c>).</param>
internal sealed record Db2RuntimeParameter(string Name);
