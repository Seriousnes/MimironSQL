using System.Linq.Expressions;
using System.Reflection;

using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.Internal;

namespace MimironSQL.EntityFrameworkCore.Query.Internal;

internal static class MimironDb2AsyncQueryAdapter
{
#pragma warning disable EF1001 // Internal EF Core API usage is isolated to this shim.
    public static TResult ExecuteAsync<TResult>(IQueryCompiler queryCompiler, Expression query, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(queryCompiler);
        ArgumentNullException.ThrowIfNull(query);

        var resultType = typeof(TResult);

        if (TryGetTaskInnerType(resultType, out var taskInnerType))
            return (TResult)CreateTaskResult(queryCompiler, query, taskInnerType);

        if (TryGetValueTaskInnerType(resultType, out var valueTaskInnerType))
            return (TResult)CreateValueTaskResult(queryCompiler, query, valueTaskInnerType);

        if (TryGetIAsyncEnumerableElementType(resultType, out var elementType))
            return (TResult)CreateAsyncEnumerableResult(queryCompiler, query, elementType, cancellationToken);

        throw new NotSupportedException($"Async query execution only supports Task<T>, ValueTask<T>, and IAsyncEnumerable<T> results; found '{resultType.FullName}'.");
    }

    public static Expression<Func<QueryContext, TResult>> PrecompileQuery<TResult>(Expression query, bool async)
    {
        ArgumentNullException.ThrowIfNull(query);

        var qc = Expression.Parameter(typeof(QueryContext), "qc");
        var context = Expression.Property(qc, nameof(QueryContext.Context));

        var getService = typeof(AccessorExtensions)
            .GetMethod(nameof(AccessorExtensions.GetService))!
            .MakeGenericMethod(typeof(IQueryCompiler));

        var compilerExpr = Expression.Call(getService, context);

        if (!async)
        {
            var execute = Expression.Call(
                instance: compilerExpr,
                method: GetExecuteMethod<TResult>(),
                arguments: Expression.Constant(query));

            return Expression.Lambda<Func<QueryContext, TResult>>(execute, qc);
        }

        var asyncCall = Expression.Call(
            GetExecuteAsyncMethod<TResult>(),
            compilerExpr,
            Expression.Constant(query));

        return Expression.Lambda<Func<QueryContext, TResult>>(asyncCall, qc);
    }

    public static TResult ExecuteAsync<TResult>(IQueryCompiler queryCompiler, Expression query)
        => ExecuteAsync<TResult>(queryCompiler, query, CancellationToken.None);

    private static MethodInfo GetExecuteMethod<TResult>()
        => typeof(IQueryCompiler).GetMethod(nameof(IQueryCompiler.Execute))!.MakeGenericMethod(typeof(TResult));

    private static MethodInfo GetExecuteAsyncMethod<TResult>()
        => typeof(MimironDb2AsyncQueryAdapter).GetMethod(nameof(ExecuteAsync), [typeof(IQueryCompiler), typeof(Expression)])!
            .MakeGenericMethod(typeof(TResult));

    private static bool TryGetTaskInnerType(Type type, out Type innerType)
    {
        if (type.IsGenericType && type.GetGenericTypeDefinition() == typeof(Task<>))
        {
            innerType = type.GetGenericArguments()[0];
            return true;
        }

        innerType = null!;
        return false;
    }

    private static bool TryGetValueTaskInnerType(Type type, out Type innerType)
    {
        if (type.IsGenericType && type.GetGenericTypeDefinition() == typeof(ValueTask<>))
        {
            innerType = type.GetGenericArguments()[0];
            return true;
        }

        innerType = null!;
        return false;
    }

    private static bool TryGetIAsyncEnumerableElementType(Type type, out Type elementType)
    {
        if (type.IsGenericType && type.GetGenericTypeDefinition() == typeof(IAsyncEnumerable<>))
        {
            elementType = type.GetGenericArguments()[0];
            return true;
        }

        elementType = null!;
        return false;
    }

    private static object CreateTaskResult(IQueryCompiler queryCompiler, Expression query, Type innerType)
    {
        var method = typeof(MimironDb2AsyncQueryAdapter)
            .GetMethod(nameof(CreateTaskResultTyped), BindingFlags.NonPublic | BindingFlags.Static)!
            .MakeGenericMethod(innerType);

        return method.Invoke(obj: null, parameters: [queryCompiler, query])!;
    }

    private static Task<T> CreateTaskResultTyped<T>(IQueryCompiler queryCompiler, Expression query)
        => Task.FromResult(queryCompiler.Execute<T>(query));

    private static object CreateValueTaskResult(IQueryCompiler queryCompiler, Expression query, Type innerType)
    {
        var method = typeof(MimironDb2AsyncQueryAdapter)
            .GetMethod(nameof(CreateValueTaskResultTyped), BindingFlags.NonPublic | BindingFlags.Static)!
            .MakeGenericMethod(innerType);

        return method.Invoke(obj: null, parameters: [queryCompiler, query])!;
    }

    private static ValueTask<T> CreateValueTaskResultTyped<T>(IQueryCompiler queryCompiler, Expression query)
        => new(queryCompiler.Execute<T>(query));

    private static object CreateAsyncEnumerableResult(IQueryCompiler queryCompiler, Expression query, Type elementType, CancellationToken cancellationToken)
    {
        var method = typeof(MimironDb2AsyncQueryAdapter)
            .GetMethod(nameof(CreateAsyncEnumerableResultTyped), BindingFlags.NonPublic | BindingFlags.Static)!
            .MakeGenericMethod(elementType);

        return method.Invoke(obj: null, parameters: [queryCompiler, query, cancellationToken])!;
    }

    private static IAsyncEnumerable<T> CreateAsyncEnumerableResultTyped<T>(IQueryCompiler queryCompiler, Expression query, CancellationToken cancellationToken)
        => new SyncAsyncEnumerable<T>(queryCompiler.Execute<IEnumerable<T>>(query), cancellationToken);

    private sealed class SyncAsyncEnumerable<T>(IEnumerable<T> source, CancellationToken cancellationToken) : IAsyncEnumerable<T>
    {
        private readonly IEnumerable<T> _source = source ?? throw new ArgumentNullException(nameof(source));
        private readonly CancellationToken _cancellationToken = cancellationToken;

        public IAsyncEnumerator<T> GetAsyncEnumerator(CancellationToken cancellationToken = default)
        {
            if (!_cancellationToken.CanBeCanceled)
                return new Enumerator(_source.GetEnumerator(), cancellationToken);

            if (!cancellationToken.CanBeCanceled)
                return new Enumerator(_source.GetEnumerator(), _cancellationToken);

            var linked = CancellationTokenSource.CreateLinkedTokenSource(_cancellationToken, cancellationToken);
            return new Enumerator(_source.GetEnumerator(), linked);
        }

        private sealed class Enumerator : IAsyncEnumerator<T>
        {
            private readonly IEnumerator<T> _enumerator;
            private readonly CancellationTokenSource? _linkedTokenSource;
            private readonly CancellationToken _cancellationToken;

            public Enumerator(IEnumerator<T> enumerator, CancellationToken cancellationToken)
            {
                _enumerator = enumerator ?? throw new ArgumentNullException(nameof(enumerator));
                _cancellationToken = cancellationToken;
            }

            public Enumerator(IEnumerator<T> enumerator, CancellationTokenSource linkedTokenSource)
            {
                _enumerator = enumerator ?? throw new ArgumentNullException(nameof(enumerator));
                _linkedTokenSource = linkedTokenSource ?? throw new ArgumentNullException(nameof(linkedTokenSource));
                _cancellationToken = linkedTokenSource.Token;
            }

            public T Current => _enumerator.Current;

            public ValueTask<bool> MoveNextAsync()
            {
                _cancellationToken.ThrowIfCancellationRequested();
                return new ValueTask<bool>(_enumerator.MoveNext());
            }

            public ValueTask DisposeAsync()
            {
                _enumerator.Dispose();
                _linkedTokenSource?.Dispose();
                return ValueTask.CompletedTask;
            }
        }
    }
#pragma warning restore EF1001
}
