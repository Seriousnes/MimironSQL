using System.Collections;
using System.Linq.Expressions;
using System.Reflection;

using Microsoft.EntityFrameworkCore.Query;

using MimironSQL.EntityFrameworkCore.Query;

namespace MimironSQL.EntityFrameworkCore.Query.Internal;

internal static class MimironDb2AsyncQueryAdapter
{
    public static TResult ExecuteAsync<TResult>(IMimironDb2QueryExecutor executor, Expression query, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(executor);
        ArgumentNullException.ThrowIfNull(query);

        var resultType = typeof(TResult);

        if (TryGetTaskInnerType(resultType, out var taskInnerType))
            return (TResult)CreateTaskResult(executor, query, taskInnerType);

        if (TryGetValueTaskInnerType(resultType, out var valueTaskInnerType))
            return (TResult)CreateValueTaskResult(executor, query, valueTaskInnerType);

        if (TryGetIAsyncEnumerableElementType(resultType, out var elementType))
            return (TResult)CreateAsyncEnumerableResult(executor, query, elementType, cancellationToken);

        throw new NotSupportedException($"Async query execution only supports Task<T>, ValueTask<T>, and IAsyncEnumerable<T> results; found '{resultType.FullName}'.");
    }

    public static Expression<Func<QueryContext, TResult>> PrecompileQuery<TResult>(IMimironDb2QueryExecutor executor, Expression query, bool async)
    {
        ArgumentNullException.ThrowIfNull(executor);
        ArgumentNullException.ThrowIfNull(query);

        var qc = Expression.Parameter(typeof(QueryContext), "qc");

        if (!async)
        {
            var execute = Expression.Call(
                instance: Expression.Constant(executor),
                method: GetExecuteMethod<TResult>(),
                arguments: Expression.Constant(query));

            return Expression.Lambda<Func<QueryContext, TResult>>(execute, qc);
        }

        var asyncCall = Expression.Call(
            GetExecuteAsyncMethod<TResult>(),
            Expression.Constant(executor),
            Expression.Constant(query));

        return Expression.Lambda<Func<QueryContext, TResult>>(asyncCall, qc);
    }

    public static TResult ExecuteAsync<TResult>(IMimironDb2QueryExecutor executor, Expression query)
        => ExecuteAsync<TResult>(executor, query, CancellationToken.None);

    private static MethodInfo GetExecuteMethod<TResult>()
        => typeof(IMimironDb2QueryExecutor).GetMethod(nameof(IMimironDb2QueryExecutor.Execute))!.MakeGenericMethod(typeof(TResult));

    private static MethodInfo GetExecuteAsyncMethod<TResult>()
        => typeof(MimironDb2AsyncQueryAdapter).GetMethod(nameof(ExecuteAsync), [typeof(IMimironDb2QueryExecutor), typeof(Expression)])!
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

    private static object CreateTaskResult(IMimironDb2QueryExecutor executor, Expression query, Type innerType)
    {
        var method = typeof(MimironDb2AsyncQueryAdapter)
            .GetMethod(nameof(CreateTaskResultTyped), BindingFlags.NonPublic | BindingFlags.Static)!
            .MakeGenericMethod(innerType);

        return method.Invoke(obj: null, parameters: [executor, query])!;
    }

    private static Task<T> CreateTaskResultTyped<T>(IMimironDb2QueryExecutor executor, Expression query)
        => Task.FromResult(executor.Execute<T>(query));

    private static object CreateValueTaskResult(IMimironDb2QueryExecutor executor, Expression query, Type innerType)
    {
        var method = typeof(MimironDb2AsyncQueryAdapter)
            .GetMethod(nameof(CreateValueTaskResultTyped), BindingFlags.NonPublic | BindingFlags.Static)!
            .MakeGenericMethod(innerType);

        return method.Invoke(obj: null, parameters: [executor, query])!;
    }

    private static ValueTask<T> CreateValueTaskResultTyped<T>(IMimironDb2QueryExecutor executor, Expression query)
        => new(executor.Execute<T>(query));

    private static object CreateAsyncEnumerableResult(IMimironDb2QueryExecutor executor, Expression query, Type elementType, CancellationToken cancellationToken)
    {
        var method = typeof(MimironDb2AsyncQueryAdapter)
            .GetMethod(nameof(CreateAsyncEnumerableResultTyped), BindingFlags.NonPublic | BindingFlags.Static)!
            .MakeGenericMethod(elementType);

        return method.Invoke(obj: null, parameters: [executor, query, cancellationToken])!;
    }

    private static IAsyncEnumerable<T> CreateAsyncEnumerableResultTyped<T>(IMimironDb2QueryExecutor executor, Expression query, CancellationToken cancellationToken)
        => new SyncAsyncEnumerable<T>(executor.Execute<IEnumerable<T>>(query), cancellationToken);

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
}
