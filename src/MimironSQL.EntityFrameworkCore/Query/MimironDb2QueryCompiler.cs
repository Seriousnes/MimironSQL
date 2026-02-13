using System.Collections.Concurrent;
using System.Linq.Expressions;
using System.Reflection;

using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.Internal;

using MimironSQL.EntityFrameworkCore.Db2.Query;
using MimironSQL.EntityFrameworkCore.Db2.Model;
using MimironSQL.EntityFrameworkCore.Db2.Schema;
using MimironSQL.EntityFrameworkCore.Query.Internal;
using MimironSQL.EntityFrameworkCore.Storage;
using MimironSQL.Formats;

namespace MimironSQL.EntityFrameworkCore.Query;

#pragma warning disable EF1001 // Internal EF Core API usage is isolated to this provider.
internal sealed class MimironDb2QueryExecutor(
    ICurrentDbContext currentDbContext,
    IMimironDb2Store store,
    IDb2ModelBinding db2ModelBinding) : IQueryCompiler
{
    private static readonly ConcurrentDictionary<(Type EntityType, Type RowType, Type ResultType), Func<MimironDb2QueryExecutor, Expression, object?>> ExecuteDelegates = new();

    private readonly DbContext _context = currentDbContext?.Context ?? throw new ArgumentNullException(nameof(currentDbContext));
    private readonly IMimironDb2Store _store = store ?? throw new ArgumentNullException(nameof(store));
    private readonly IDb2ModelBinding _db2ModelBinding = db2ModelBinding ?? throw new ArgumentNullException(nameof(db2ModelBinding));

    public Func<QueryContext, TResult> CreateCompiledQuery<TResult>(Expression query)
    {
        ArgumentNullException.ThrowIfNull(query);

        // Don't capture this scoped service in a delegate that EF Core may cache.
        return qc => qc.Context.GetService<IQueryCompiler>().Execute<TResult>(query);
    }

    public Func<QueryContext, TResult> CreateCompiledAsyncQuery<TResult>(Expression query)
    {
        ArgumentNullException.ThrowIfNull(query);

        return qc =>
        {
            var compiler = qc.Context.GetService<IQueryCompiler>();
            return MimironDb2AsyncQueryAdapter.ExecuteAsync<TResult>(compiler, query);
        };
    }

    public TResult ExecuteAsync<TResult>(Expression query, CancellationToken cancellationToken)
        => MimironDb2AsyncQueryAdapter.ExecuteAsync<TResult>(this, query, cancellationToken);

    public Expression<Func<QueryContext, TResult>> PrecompileQuery<TResult>(Expression query, bool async)
        => MimironDb2AsyncQueryAdapter.PrecompileQuery<TResult>(query, async);

    public TResult Execute<TResult>(Expression query)
    {
        ArgumentNullException.ThrowIfNull(query);

        query = MimironDb2EfExpressionNormalizer.Normalize(query);

        var rootEntityType = GetRootEntityClrType(query);

        var efEntityType = _context.Model.FindEntityType(rootEntityType)
            ?? throw new NotSupportedException($"Entity type '{rootEntityType.FullName}' is not part of the EF model.");

        // This provider currently uses RowHandle for all shipped formats.
        // Avoid opening/caching full tables just to discover the row type.
        var rowType = typeof(RowHandle);

        var result = ExecuteDelegates.GetOrAdd((rootEntityType, rowType, typeof(TResult)), static key =>
        {
            var method = typeof(MimironDb2QueryExecutor)
                .GetMethod(nameof(ExecuteTyped), BindingFlags.NonPublic | BindingFlags.Instance)!
                .MakeGenericMethod(key.EntityType, key.RowType, key.ResultType);

            return method.CreateDelegate<Func<MimironDb2QueryExecutor, Expression, object?>>();
        })(this, query);

        return (TResult)result!;
    }

    private object? ExecuteTyped<TEntity, TRow, TResult>(Expression query)
        where TEntity : class
        where TRow : struct, IRowHandle
    {
        var model = _db2ModelBinding.GetBinding();

        // Step 1: Preprocess expression — strip Include/ThenInclude and EF query modifiers.
        var preprocessed = Db2ExpressionPreprocessor.Preprocess(query);

        // Step 2: Try PK key-lookup fast path.
        if (typeof(TRow) == typeof(RowHandle)
            && TryGetPrimaryKeyMemberName(model, typeof(TEntity), out var pkMemberName)
            && TryGetKeyLookupInfo<TEntity>(preprocessed.CleanedExpression, pkMemberName, out var keyLookup))
        {
            IDb2EntityFactory keyLookupEntityFactory = new EfLazyLoadingProxyDb2EntityFactory(_context, new ReflectionDb2EntityFactory());

            var efEntityType2 = _context.Model.FindEntityType(typeof(TEntity))
                ?? throw new NotSupportedException($"Entity type '{typeof(TEntity).FullName}' is not part of the EF model.");

            var tableName2 = efEntityType2.GetTableName() ?? typeof(TEntity).Name;

            var (rootFile, rootSchema) = _store.OpenTableWithSchema<RowHandle>(tableName2);

            var rootEntities = MaterializeByIdsFromOpenFile<TEntity>(rootFile, rootSchema, tableName2, keyLookup.Ids, keyLookup.TakeCount, model, keyLookupEntityFactory);

            // Apply includes.
            IEnumerable<TEntity> current = rootEntities;
            for (var i = 0; i < preprocessed.IncludeChains.Count; i++)
            {
                current = Db2IncludeChainExecutor.Apply<TEntity, RowHandle>(
                    current,
                    model,
                    name => _store.OpenTableWithSchema<RowHandle>(name),
                    preprocessed.IncludeChains[i],
                    keyLookupEntityFactory);
            }

            // Materialize the included entities, substitute the root in the residual
            // expression, and let LINQ-to-Objects handle Select + terminal operators.
            var entities = current.ToList();
            var residual = SubstituteRoot<TEntity>(keyLookup.ResidualExpression, entities);
            return CompileAndExecute<TResult>(residual);
        }

        // Step 3: Provider path — let Db2QueryProvider handle optimizations.
        var efEntityType = _context.Model.FindEntityType(typeof(TEntity))
            ?? throw new NotSupportedException($"Entity type '{typeof(TEntity).FullName}' is not part of the EF model.");

        var tableName = efEntityType.GetTableName() ?? typeof(TEntity).Name;

        var (file, schema) = _store.OpenTableWithSchema<TRow>(tableName);

        (IDb2File<TRow> File, Db2TableSchema Schema) TableResolver(string name)
            => _store.OpenTableWithSchema<TRow>(name);

        IDb2EntityFactory queryEntityFactory = new EfLazyLoadingProxyDb2EntityFactory(_context, new ReflectionDb2EntityFactory());
        var provider = new Db2QueryProvider<TEntity, TRow>(file, model, TableResolver, queryEntityFactory);

        model.GetEntityType(typeof(TEntity)).WithSchema(tableName, schema);
        var rootQueryable = new Db2Queryable<TEntity>(provider);

        var rewritten = new RootQueryRewriter<TEntity>(rootQueryable).Visit(query);
        return provider.Execute<TResult>(rewritten!);
    }

    // ──────── Key-lookup helpers ────────

    private static bool TryGetPrimaryKeyMemberName(Db2ModelBinding model, Type entityClrType, out string memberName)
    {
        var entityType = model.GetEntityType(entityClrType);
        memberName = entityType.PrimaryKeyMember.Name;
        return true;
    }

    /// <summary>
    /// Walks the (pre-processed / Include-stripped) expression tree to detect a
    /// PK-based key lookup query.  If successful, returns the extracted IDs,
    /// optional Take count, and a <em>residual</em> expression — the original
    /// expression with the PK Where and Take calls stripped so that LINQ-to-Objects
    /// can evaluate the rest (Select, terminal operators, etc.).
    /// </summary>
    private static bool TryGetKeyLookupInfo<TEntity>(
        Expression cleanedExpression,
        string pkMemberName,
        out KeyLookupInfo keyLookup)
    {
        // Walk the expression chain and classify nodes.
        var current = cleanedExpression;
        MethodCallExpression? pkWhereNode = null;
        MethodCallExpression? takeNode = null;
        var rootEntityWhereCount = 0;
        var hasSkip = false;
        var selectCount = 0;

        // Peel off a terminal operator (First, Single, Any, Count, All) at the outermost level.
        // These are valid in the key-lookup path and will be kept in the residual expression.
        if (current is MethodCallExpression { Method.DeclaringType: { } declaring } outermost
            && declaring == typeof(Queryable)
            && outermost.Method.Name is nameof(Queryable.First) or nameof(Queryable.FirstOrDefault)
                or nameof(Queryable.Single) or nameof(Queryable.SingleOrDefault)
                or nameof(Queryable.Any) or nameof(Queryable.All)
                or nameof(Queryable.Count))
        {
            current = outermost.Arguments[0];
        }

        // Walk outermost → innermost along the source chain.
        while (current is MethodCallExpression m)
        {
            if (m.Method.DeclaringType == typeof(Queryable))
            {
                switch (m.Method.Name)
                {
                    case nameof(Queryable.Where):
                        {
                            var predicate = Db2ExpressionPreprocessor.UnquoteLambda(m.Arguments[1]);
                            if (predicate.Parameters is { Count: 1 } && predicate.Parameters[0].Type == typeof(TEntity))
                            {
                                rootEntityWhereCount++;

                                if (TryExtractPkIds<TEntity>((Expression<Func<TEntity, bool>>)predicate, pkMemberName, out _))
                                    pkWhereNode = m;
                            }

                            current = m.Arguments[0];
                            continue;
                        }

                    case nameof(Queryable.Skip):
                        hasSkip = true;
                        current = m.Arguments[0];
                        continue;

                    case nameof(Queryable.Take):
                        {
                            if (m.Arguments[1] is ConstantExpression { Value: int count } && count >= 0)
                                takeNode = m;

                            current = m.Arguments[0];
                            continue;
                        }

                    case nameof(Queryable.Select):
                        selectCount++;
                        if (selectCount > 1)
                        {
                            keyLookup = default;
                            return false;
                        }

                        // Validate the select parameter type matches TEntity.
                        var selector = Db2ExpressionPreprocessor.UnquoteLambda(m.Arguments[1]);
                        if (selector is not { Parameters.Count: 1 } || selector.Parameters[0].Type != typeof(TEntity))
                        {
                            keyLookup = default;
                            return false;
                        }

                        current = m.Arguments[0];
                        continue;

                    default:
                        current = m.Arguments[0];
                        continue;
                }
            }

            // Non-Queryable method in the chain — bail.
            if (m.Arguments.Count > 0)
                current = m.Arguments[0];
            else
                break;
        }

        // Disqualifiers:
        // - Skip present → key-lookup cannot reorder correctly
        // - Not exactly one root-entity Where with PK access
        if (hasSkip || rootEntityWhereCount != 1 || pkWhereNode is null)
        {
            keyLookup = default;
            return false;
        }

        // Extract the IDs from the PK Where predicate.
        var pkPredicate = (Expression<Func<TEntity, bool>>)Db2ExpressionPreprocessor.UnquoteLambda(pkWhereNode.Arguments[1]);
        if (!TryExtractPkIds<TEntity>(pkPredicate, pkMemberName, out var ids))
        {
            keyLookup = default;
            return false;
        }

        int? takeCount = null;
        if (takeNode is not null && takeNode.Arguments[1] is ConstantExpression { Value: int tc })
            takeCount = tc;

        // Build the residual expression by stripping the PK Where and Take nodes.
        var nodesToStrip = new HashSet<Expression>(ReferenceEqualityComparer.Instance) { pkWhereNode };
        if (takeNode is not null)
            nodesToStrip.Add(takeNode);

        var residual = new OperationStripper(nodesToStrip).Visit(cleanedExpression);

        keyLookup = new KeyLookupInfo(ids, takeCount, residual);
        return true;
    }

    private static IReadOnlyList<TEntity> MaterializeByIdsFromOpenFile<TEntity>(
        IDb2File<RowHandle> file,
        Db2TableSchema schema,
        string tableName,
        IReadOnlyList<int> ids,
        int? takeCount,
        Db2ModelBinding model,
        IDb2EntityFactory entityFactory)
        where TEntity : class
    {
        if (takeCount is 0 || ids.Count == 0)
            return Array.Empty<TEntity>();

        if (takeCount is < 0)
            throw new ArgumentOutOfRangeException(nameof(takeCount), "Take count cannot be negative.");

        var maxCount = takeCount ?? int.MaxValue;

        var handles = new List<RowHandle>(capacity: Math.Min(ids.Count, maxCount));
        for (var i = 0; i < ids.Count; i++)
        {
            if (handles.Count >= maxCount)
                break;

            if (!file.TryGetRowById(ids[i], out var handle))
                continue;

            handles.Add(handle);
        }

        if (handles.Count == 0)
            return Array.Empty<TEntity>();

        handles.Sort(static (a, b) =>
        {
            var section = a.SectionIndex.CompareTo(b.SectionIndex);
            if (section != 0)
                return section;

            return a.RowIndexInSection.CompareTo(b.RowIndexInSection);
        });

        var db2EntityType = model.GetEntityType(typeof(TEntity)).WithSchema(tableName, schema);
        var materializer = new Db2EntityMaterializer<TEntity>(model, db2EntityType, entityFactory);

        var results = new List<TEntity>(handles.Count);
        for (var i = 0; i < handles.Count; i++)
            results.Add(materializer.Materialize(file, handles[i]));

        return results;
    }

    // ──────── Expression tree helpers ────────

    /// <summary>
    /// Replaces the root <see cref="EntityQueryRootExpression"/> in <paramref name="expression"/>
    /// with a <c>Constant(entities.AsQueryable())</c>, producing an expression that
    /// LINQ-to-Objects can evaluate directly.
    /// </summary>
    private static Expression SubstituteRoot<TEntity>(Expression expression, List<TEntity> entities)
        where TEntity : class
    {
        var queryable = entities.AsQueryable();
        return new RootQueryRewriter<TEntity>(queryable).Visit(expression);
    }

    /// <summary>
    /// Compiles a self-contained expression (no parameters) and executes it,
    /// returning the result as <typeparamref name="TResult"/>.
    /// Handles type conversions (e.g. IQueryable&lt;T&gt; → IEnumerable&lt;T&gt;).
    /// </summary>
    internal static TResult CompileAndExecute<TResult>(Expression expression)
    {
        // Residual expressions are executed via LINQ-to-Objects after we materialize entities.
        // EF sometimes injects EF.Property<T>(entity, "Member") calls (e.g., in Find/PK predicates).
        // Those must be rewritten to normal member access before compilation.
        var body = RewriteEfPropertyCalls(expression);

        // The expression might produce IQueryable<T> while TResult is IEnumerable<T>.
        // Insert a Convert when the types don't match exactly but are assignable.
        if (body.Type != typeof(TResult))
            body = Expression.Convert(body, typeof(TResult));

        return Expression.Lambda<Func<TResult>>(body).Compile()();
    }

    private static Expression RewriteEfPropertyCalls(Expression expression)
        => new EfPropertyToMemberAccessRewriter().Visit(expression);

    private sealed class EfPropertyToMemberAccessRewriter : ExpressionVisitor
    {
        protected override Expression VisitMethodCall(MethodCallExpression node)
        {
            // EF.Property<T>(entity, "Member")
            if (node.Method.Name == "Property"
                && node.Method.DeclaringType is { Name: "EF", Namespace: "Microsoft.EntityFrameworkCore" }
                && node.Arguments.Count == 2)
            {
                var entity = Visit(node.Arguments[0]);
                if (node.Arguments[1] is ConstantExpression { Value: string memberName })
                {
                    // Prefer field/property access; fallback to reflection-based property access.
                    var member = entity.Type.GetMember(memberName, BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic)
                        .FirstOrDefault(m => m is PropertyInfo or FieldInfo);

                    if (member is PropertyInfo pi)
                        return Expression.Property(entity, pi);

                    if (member is FieldInfo fi)
                        return Expression.Field(entity, fi);
                }
            }

            return base.VisitMethodCall(node);
        }
    }

    // ──────── PK extraction ────────

    private static bool TryExtractPkIds<TEntity>(Expression<Func<TEntity, bool>> predicate, string pkMemberName, out int[] ids)
    {
        // Supports shapes like:
        //  - x => x.Id == 123
        //  - x => 123 == x.Id
        //  - x => EF.Property<int>(x, "Id") == 123
        //  - x => new[] { 1, 2, 3 }.Contains(x.Id)
        //  - x => x.Id == 1 || x.Id == 2

        static Expression StripConvert(Expression e)
            => e is UnaryExpression { NodeType: ExpressionType.Convert or ExpressionType.ConvertChecked } u ? u.Operand : e;

        ids = [];

        var raw = new List<int>();
        if (!TryExtractIdsFromExpression(StripConvert(predicate.Body), predicate.Parameters[0], pkMemberName, raw))
            return false;

        if (raw.Count == 0)
            return false;

        var seen = new HashSet<int>();
        var orderedDistinct = new List<int>(raw.Count);
        for (var i = 0; i < raw.Count; i++)
        {
            var id = raw[i];
            if (seen.Add(id))
                orderedDistinct.Add(id);
        }

        ids = [.. orderedDistinct];
        return true;

        static bool TryExtractIdsFromExpression(Expression expr, ParameterExpression param, string pkMemberName, List<int> ids)
        {
            expr = StripConvert(expr);

            switch (expr)
            {
                case BinaryExpression { NodeType: ExpressionType.Equal } eq:
                    {
                        var left = StripConvert(eq.Left);
                        var right = StripConvert(eq.Right);

                        if (IsKeyAccess(left, param, pkMemberName) && TryEvaluateInt(right, param, out var rightId))
                        {
                            ids.Add(rightId);
                            return true;
                        }

                        if (IsKeyAccess(right, param, pkMemberName) && TryEvaluateInt(left, param, out var leftId))
                        {
                            ids.Add(leftId);
                            return true;
                        }

                        return false;
                    }

                case BinaryExpression { NodeType: ExpressionType.OrElse } or:
                    {
                        // Preserve evaluation order: left ids, then right ids.
                        return TryExtractIdsFromExpression(or.Left, param, pkMemberName, ids)
                               && TryExtractIdsFromExpression(or.Right, param, pkMemberName, ids);
                    }

                case MethodCallExpression call:
                    return TryExtractContains(call, param, pkMemberName, ids);

                default:
                    return false;
            }
        }

        static bool TryExtractContains(MethodCallExpression call, ParameterExpression param, string pkMemberName, List<int> ids)
        {
            if (!string.Equals(call.Method.Name, nameof(Enumerable.Contains), StringComparison.Ordinal))
                return false;

            Expression? collectionExpr = null;
            Expression? valueExpr = null;

            // Enumerable.Contains(collection, value)
            if (call.Method.DeclaringType == typeof(Enumerable) && call.Arguments.Count == 2)
            {
                collectionExpr = call.Arguments[0];
                valueExpr = call.Arguments[1];
            }
            // instance.Contains(value)
            else if (call.Object is not null && call.Arguments.Count == 1)
            {
                collectionExpr = call.Object;
                valueExpr = call.Arguments[0];
            }
            // Static Contains with two args (e.g. List.Contains as static unlikely but handle).
            else if (call.Object is null && call.Arguments.Count == 2)
            {
                collectionExpr = call.Arguments[0];
                valueExpr = call.Arguments[1];
            }

            if (collectionExpr is null || valueExpr is null)
                return false;

            valueExpr = StripConvert(valueExpr);

            if (!IsKeyAccess(valueExpr, param, pkMemberName))
                return false;

            if (!TryEvaluateIntSequence(collectionExpr, param, out var values))
                return false;

            for (var i = 0; i < values.Count; i++)
                ids.Add(values[i]);

            return true;
        }

        static bool TryEvaluateIntSequence(Expression expr, ParameterExpression param, out IReadOnlyList<int> values)
        {
            expr = StripConvert(expr);

            // new[] { ... }
            if (expr is NewArrayExpression newArray)
            {
                var list = new List<int>(newArray.Expressions.Count);
                for (var i = 0; i < newArray.Expressions.Count; i++)
                {
                    if (!TryEvaluateInt(newArray.Expressions[i], param, out var value))
                    {
                        values = [];
                        return false;
                    }

                    list.Add(value);
                }

                values = list;
                return true;
            }

            if (ReferencesParameter(expr, param))
            {
                values = [];
                return false;
            }

            try
            {
                var converted = Expression.Convert(expr, typeof(object));
                var fn = Expression.Lambda<Func<object>>(converted).Compile();
                var obj = fn();
                if (obj is IEnumerable<int> ints)
                {
                    values = ints as IReadOnlyList<int> ?? ints.ToArray();
                    return true;
                }
            }
            catch
            {
                // ignore
            }

            values = [];
            return false;
        }

        static bool TryEvaluateInt(Expression expr, ParameterExpression param, out int value)
        {
            expr = StripConvert(expr);

            if (expr is ConstantExpression { Value: int i })
            {
                value = i;
                return true;
            }

            if (ReferencesParameter(expr, param))
            {
                value = 0;
                return false;
            }

            try
            {
                var fn = Expression.Lambda<Func<int>>(Expression.Convert(expr, typeof(int))).Compile();
                value = fn();
                return true;
            }
            catch
            {
                value = 0;
                return false;
            }
        }

        static bool ReferencesParameter(Expression expr, ParameterExpression param)
        {
            var visitor = new ParameterReferenceVisitor(param);
            visitor.Visit(expr);
            return visitor.Found;
        }
    }

    private static bool IsKeyAccess(Expression expr, ParameterExpression param, string pkMemberName)
    {
        if (expr is MemberExpression { Member: { Name: var name }, Expression: var instance } && instance == param && name == pkMemberName)
            return true;

        // EF.Property<T>(entity, "Id")
        if (expr is MethodCallExpression { Method: { Name: "Property", DeclaringType: { Name: "EF", Namespace: "Microsoft.EntityFrameworkCore" } }, Arguments: [var entityExpr, var nameExpr] })
        {
            if (entityExpr == param && nameExpr is ConstantExpression { Value: string s } && s == pkMemberName)
                return true;
        }

        return false;
    }

    // ──────── Supporting types ────────

    private readonly record struct KeyLookupInfo(
        int[] Ids,
        int? TakeCount,
        Expression ResidualExpression);

    /// <summary>
    /// Strips specific <see cref="MethodCallExpression"/> nodes from an expression tree
    /// by replacing them with their source argument (first argument for Queryable extension methods).
    /// </summary>
    internal sealed class OperationStripper(HashSet<Expression> toStrip) : ExpressionVisitor
    {
        protected override Expression VisitMethodCall(MethodCallExpression node)
        {
            if (toStrip.Contains(node))
                return Visit(node.Arguments[0]);

            return base.VisitMethodCall(node);
        }
    }

    private sealed class RootQueryRewriter<TEntity>(IQueryable<TEntity> root) : ExpressionVisitor
    {
        private readonly IQueryable<TEntity> _root = root;

        protected override Expression VisitExtension(Expression node)
        {
            if (node is EntityQueryRootExpression eqr && eqr.EntityType.ClrType == typeof(TEntity))
                return Expression.Constant(_root, typeof(IQueryable<TEntity>));

            return base.VisitExtension(node);
        }

        protected override Expression VisitConstant(ConstantExpression node)
        {
            if (node.Value is IQueryable q
                && q.ElementType == typeof(TEntity)
                && q.Expression is EntityQueryRootExpression eqr
                && eqr.EntityType.ClrType == typeof(TEntity))
            {
                return Expression.Constant(_root, typeof(IQueryable<TEntity>));
            }

            return base.VisitConstant(node);
        }
    }

    private sealed class ParameterReferenceVisitor(ParameterExpression parameter) : ExpressionVisitor
    {
        private readonly ParameterExpression _parameter = parameter;
        public bool Found { get; private set; }

        public override Expression? Visit(Expression? node)
        {
            if (Found || node is null)
                return node;

            return base.Visit(node);
        }

        protected override Expression VisitParameter(ParameterExpression node)
        {
            if (node == _parameter)
                Found = true;

            return node;
        }
    }

    private static Type GetRootEntityClrType(Expression expression)
    {
        var current = expression;
        while (current is MethodCallExpression m)
            current = m.Arguments[0];

        return GetSequenceElementType(current.Type) ?? throw new NotSupportedException("Unable to determine root entity type for this query.");
    }

    private static Type? GetSequenceElementType(Type sequenceType)
        => sequenceType.IsGenericType && sequenceType.GetGenericTypeDefinition() == typeof(IEnumerable<>)
            ? sequenceType.GetGenericArguments()[0]
            : sequenceType.GetInterfaces()
                .FirstOrDefault(i => i.IsGenericType && i.GetGenericTypeDefinition() == typeof(IEnumerable<>))
                ?.GetGenericArguments()[0];
}

#pragma warning restore EF1001
