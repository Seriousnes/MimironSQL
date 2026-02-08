using System.Linq.Expressions;

namespace MimironSQL.EntityFrameworkCore.Query;

internal interface IMimironDb2QueryExecutor
{
    TResult Execute<TResult>(Expression query);
}
