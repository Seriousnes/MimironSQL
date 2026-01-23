using MimironSQL.Db2.Schema;
using MimironSQL.Db2.Wdc5;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;

namespace MimironSQL.Db2.Query;

internal sealed class Db2QueryProvider<TEntity>(Wdc5File file, Db2TableSchema schema) : IQueryProvider
{
    private readonly Db2EntityMaterializer<TEntity> _materializer = new(schema);

    public IQueryable CreateQuery(Expression expression)
    {
        ArgumentNullException.ThrowIfNull(expression);

        var elementType = expression.Type.GetGenericArguments().FirstOrDefault() ?? typeof(object);
        var queryableType = typeof(Db2Queryable<>).MakeGenericType(elementType);
        return (IQueryable)Activator.CreateInstance(queryableType, this, expression)!;
    }

    public IQueryable<TElement> CreateQuery<TElement>(Expression expression)
    {
        ArgumentNullException.ThrowIfNull(expression);
        return new Db2Queryable<TElement>(this, expression);
    }

    public object? Execute(Expression expression)
    {
        ArgumentNullException.ThrowIfNull(expression);
        var resultType = expression.Type;

        if (TryGetEnumerableElementType(resultType, out var elementType))
        {
            var method = typeof(Db2QueryProvider<TEntity>).GetMethod(nameof(ExecuteEnumerable), System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic)!;
            var generic = method.MakeGenericMethod(elementType);
            return generic.Invoke(this, [expression]);
        }

        var scalarMethod = typeof(Db2QueryProvider<TEntity>).GetMethod(nameof(ExecuteScalar), System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic)!;
        var scalarGeneric = scalarMethod.MakeGenericMethod(resultType);
        return scalarGeneric.Invoke(this, [expression]);
    }

    public TResult Execute<TResult>(Expression expression)
        => (TResult)Execute(expression)!;

    private IEnumerable<TElement> ExecuteEnumerable<TElement>(Expression expression)
    {
        var pipeline = Db2QueryPipeline.Parse(expression);

        var ops = pipeline.Operations.ToList();

        var selectIndex = ops.FindIndex(op => op is Db2SelectOperation);
        if (selectIndex < 0)
            selectIndex = ops.Count;

        var preEntityWhere = ops
            .Take(selectIndex)
            .OfType<Db2WhereOperation>()
            .Where(op => op.Predicate.Parameters.Count == 1 && op.Predicate.Parameters[0].Type == typeof(TEntity))
            .Select(op => (Expression<Func<TEntity, bool>>)op.Predicate)
            .ToArray();

        var preTake = ops
            .Take(selectIndex)
            .OfType<Db2TakeOperation>()
            .FirstOrDefault();

        var stopAfter = preTake?.Count;

        var postOps = new List<Db2QueryOperation>(ops.Count);
        for (var i = 0; i < ops.Count; i++)
        {
            var op = ops[i];

            if (op == preTake)
                continue;

            if (i < selectIndex && op is Db2WhereOperation w && w.Predicate.Parameters.Count == 1 && w.Predicate.Parameters[0].Type == typeof(TEntity))
                continue;

            postOps.Add(op);
        }

        IEnumerable<TEntity> baseEntities = EnumerateEntities(preEntityWhere, stopAfter);

        IEnumerable current = baseEntities;
        var currentElementType = typeof(TEntity);

        foreach (var op in postOps)
        {
            switch (op)
            {
                case Db2WhereOperation where:
                    current = ApplyWhere(current, currentElementType, where.Predicate);
                    break;
                case Db2SelectOperation select:
                    current = ApplySelect(current, currentElementType, select.Selector);
                    currentElementType = select.Selector.ReturnType;
                    break;
                case Db2TakeOperation take:
                    current = ApplyTake(current, currentElementType, take.Count);
                    break;
                default:
                    throw new NotSupportedException($"Unsupported query operation: {op.GetType().Name}.");
            }
        }

        return (IEnumerable<TElement>)current;
    }

    private TResult ExecuteScalar<TResult>(Expression expression)
    {
        var pipeline = Db2QueryPipeline.Parse(expression);
        if (pipeline.FinalOperator is not Db2FinalOperator.FirstOrDefault)
            throw new NotSupportedException($"Unsupported scalar operator: {pipeline.FinalOperator}.");

        var elementType = pipeline.FinalElementType;
        var enumerable = Execute(expressionWithFinalStripped: pipeline.ExpressionWithoutFinalOperator, elementType);

        var firstOrDefault = typeof(Enumerable)
            .GetMethods(System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.Static)
            .Single(m => m.Name == nameof(Enumerable.FirstOrDefault) && m.GetParameters().Length == 1)
            .MakeGenericMethod(elementType);

        return (TResult)firstOrDefault.Invoke(null, [enumerable])!;
    }

    private object Execute(Expression expressionWithFinalStripped, Type elementType)
    {
        var exec = typeof(Db2QueryProvider<TEntity>).GetMethod(nameof(ExecuteEnumerable), System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic)!;
        var generic = exec.MakeGenericMethod(elementType);
        return generic.Invoke(this, [expressionWithFinalStripped])!;
    }

    private IEnumerable<TEntity> EnumerateEntities(Expression<Func<TEntity, bool>>[] predicates, int? take)
    {
        var rowPredicates = new List<Func<Wdc5Row, bool>>();
        var entityPredicates = new List<Func<TEntity, bool>>();

        foreach (var predicate in predicates)
        {
            if (Db2RowPredicateCompiler.TryCompile(file, schema, predicate, out var rowPredicate))
                rowPredicates.Add(rowPredicate);
            else
                entityPredicates.Add(predicate.Compile());
        }

        var yielded = 0;
        foreach (var row in file.EnumerateRows())
        {
            var ok = true;
            foreach (var p in rowPredicates)
            {
                if (!p(row))
                {
                    ok = false;
                    break;
                }
            }

            if (!ok)
                continue;

            var entity = _materializer.Materialize(row);

            foreach (var p in entityPredicates)
            {
                if (!p(entity))
                {
                    ok = false;
                    break;
                }
            }

            if (!ok)
                continue;

            yield return entity;

            yielded++;
            if (take.HasValue && yielded >= take.Value)
                yield break;
        }
    }

    private static IEnumerable ApplyWhere(IEnumerable source, Type sourceElementType, LambdaExpression predicate)
    {
        var typedPredicate = predicate.Compile();

        var where = typeof(Enumerable)
            .GetMethods(System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.Static)
            .Single(m =>
                m.Name == nameof(Enumerable.Where) &&
                m.GetParameters().Length == 2 &&
                m.GetParameters()[1].ParameterType.IsGenericType &&
                m.GetParameters()[1].ParameterType.GetGenericArguments().Length == 2)
            .MakeGenericMethod(sourceElementType);

        return (IEnumerable)where.Invoke(null, [source, typedPredicate])!;
    }

    private static IEnumerable ApplySelect(IEnumerable source, Type sourceElementType, LambdaExpression selector)
    {
        var resultType = selector.ReturnType;
        var typedSelector = selector.Compile();

        var select = typeof(Enumerable)
            .GetMethods(System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.Static)
            .Single(m =>
                m.Name == nameof(Enumerable.Select) &&
                m.GetParameters().Length == 2 &&
                m.GetParameters()[1].ParameterType.IsGenericType &&
                m.GetParameters()[1].ParameterType.GetGenericArguments().Length == 2)
            .MakeGenericMethod(sourceElementType, resultType);

        return (IEnumerable)select.Invoke(null, [source, typedSelector])!;
    }

    private static IEnumerable ApplyTake(IEnumerable source, Type sourceElementType, int count)
    {
        var take = typeof(Enumerable)
            .GetMethods(System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.Static)
            .Single(m =>
                m.Name == nameof(Enumerable.Take) &&
                m.GetParameters().Length == 2 &&
                m.GetParameters()[1].ParameterType == typeof(int))
            .MakeGenericMethod(sourceElementType);

        return (IEnumerable)take.Invoke(null, [source, count])!;
    }

    private static bool TryGetEnumerableElementType(Type type, out Type elementType)
    {
        if (type.IsGenericType && type.GetGenericTypeDefinition() == typeof(IEnumerable<>))
        {
            elementType = type.GetGenericArguments()[0];
            return true;
        }

        if (type.IsGenericType && type.GetGenericTypeDefinition() == typeof(IQueryable<>))
        {
            elementType = type.GetGenericArguments()[0];
            return true;
        }

        var ienum = type.GetInterfaces().FirstOrDefault(i => i.IsGenericType && i.GetGenericTypeDefinition() == typeof(IEnumerable<>));
        if (ienum is not null)
        {
            elementType = ienum.GetGenericArguments()[0];
            return true;
        }

        elementType = typeof(object);
        return false;
    }
}
