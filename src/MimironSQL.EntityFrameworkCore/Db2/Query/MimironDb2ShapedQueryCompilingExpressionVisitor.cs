using System.Linq.Expressions;
using System.Reflection;

using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Storage;

using MimironSQL.EntityFrameworkCore.Db2.Query.Expressions;
using MimironSQL.EntityFrameworkCore.Db2.Schema;
using MimironSQL.EntityFrameworkCore.Infrastructure;
using MimironSQL.Formats;
using MimironSQL.Formats.Wdc5;
using MimironSQL.Providers;

namespace MimironSQL.EntityFrameworkCore.Db2.Query;

internal sealed class MimironDb2ShapedQueryCompilingExpressionVisitor(
    ShapedQueryCompilingExpressionVisitorDependencies dependencies,
    QueryCompilationContext queryCompilationContext)
    : ShapedQueryCompilingExpressionVisitor(dependencies, queryCompilationContext)
{
    private static readonly MethodInfo TableMethodInfo = typeof(MimironDb2ShapedQueryCompilingExpressionVisitor)
        .GetTypeInfo()
        .GetDeclaredMethod(nameof(Table))!;

    private static readonly MethodInfo QueryMethodInfo = typeof(MimironDb2ShapedQueryCompilingExpressionVisitor)
        .GetTypeInfo()
        .GetDeclaredMethod(nameof(Query))!;

    protected override Expression VisitExtension(Expression extensionExpression)
    {
        if (extensionExpression is Db2QueryExpression db2QueryExpression)
        {
            return Expression.Call(
                TableMethodInfo,
                QueryCompilationContext.QueryContextParameter,
                Expression.Constant(db2QueryExpression));
        }

        return base.VisitExtension(extensionExpression);
    }

    protected override Expression VisitShapedQuery(ShapedQueryExpression shapedQueryExpression)
    {
        ArgumentNullException.ThrowIfNull(shapedQueryExpression);

        if (shapedQueryExpression.QueryExpression is not Db2QueryExpression)
        {
            throw new NotSupportedException(
                "MimironDb2 shaped query compilation currently requires Db2QueryExpression as the query root.");
        }

        // Execute Db2QueryExpression -> IEnumerable<ValueBuffer>.
        var valueBuffers = Visit(shapedQueryExpression.QueryExpression);

        // Shaper compilation:
        // - Inject entity materializers (EF Core transforms StructuralTypeShaperExpression into executable materialization).
        // - Replace ProjectionBindingExpression with a ValueBuffer parameter.
        var valueBufferParameter = Expression.Parameter(typeof(ValueBuffer), "valueBuffer");

        var shaperBody = shapedQueryExpression.ShaperExpression;
        shaperBody = InjectStructuralTypeMaterializers(shaperBody);
        shaperBody = new ProjectionBindingRemovingVisitor(valueBufferParameter).Visit(shaperBody);
        shaperBody = new IncludeExpressionRemovingVisitor(QueryCompilationContext.QueryContextParameter).Visit(shaperBody);

        var resultType = shaperBody.Type;
        var shaperDelegateType = typeof(Func<,,>).MakeGenericType(typeof(QueryContext), typeof(ValueBuffer), resultType);
        var shaperLambda = Expression.Lambda(
            shaperDelegateType,
            shaperBody,
            QueryCompilationContext.QueryContextParameter,
            valueBufferParameter);

        var shaperConstant = Expression.Constant(shaperLambda.Compile(), shaperDelegateType);

        // Sync-only execution for now.
        // TODO: add async query execution support.
        return Expression.Call(
            QueryMethodInfo.MakeGenericMethod(resultType),
            QueryCompilationContext.QueryContextParameter,
            valueBuffers,
            shaperConstant);
    }

    private sealed class ProjectionBindingRemovingVisitor(ParameterExpression valueBufferParameter) : ExpressionVisitor
    {
        private readonly ParameterExpression _valueBufferParameter = valueBufferParameter;

        protected override Expression VisitExtension(Expression node)
        {
            if (node is ProjectionBindingExpression)
                return _valueBufferParameter;

            return base.VisitExtension(node);
        }
    }

    private sealed class IncludeExpressionRemovingVisitor(ParameterExpression queryContextParameter) : ExpressionVisitor
    {
        private readonly ParameterExpression _queryContextParameter = queryContextParameter;

        protected override Expression VisitExtension(Expression node)
        {
            // Correctness-first include execution:
            // EF Core may leave IncludeExpression nodes in the shaper, which are not reducible and cannot be compiled.
            // We rewrite them into explicit sync loads.
            // TODO: replace N+1 include loading with provider-native/batched execution.
            if (node is IncludeExpression includeExpression)
            {
                var entityExpression = Visit(includeExpression.EntityExpression);

                if (includeExpression.Navigation is not INavigationBase navigation)
                    throw new NotSupportedException("MimironDb2 IncludeExpression without INavigationBase navigation is not supported.");

                var entityVariable = Expression.Variable(entityExpression.Type, "entity");
                var assign = Expression.Assign(entityVariable, entityExpression);

                var dbContextExpression = Expression.Property(_queryContextParameter, nameof(QueryContext.Context));

                var loadCall = Expression.Call(
                    LoadNavigationMethodInfo,
                    dbContextExpression,
                    Expression.Convert(entityVariable, typeof(object)),
                    Expression.Constant(navigation.Name),
                    Expression.Constant(navigation.IsCollection));

                // IncludeExpression returns the entity.
                return Expression.Block(
                    [entityVariable],
                    assign,
                    Expression.Condition(
                        Expression.Equal(Expression.Convert(entityVariable, typeof(object)), Expression.Constant(null, typeof(object))),
                        Expression.Empty(),
                        loadCall),
                    entityVariable);
            }

            return base.VisitExtension(node);
        }

        private static readonly MethodInfo LoadNavigationMethodInfo = typeof(IncludeExpressionRemovingVisitor)
            .GetTypeInfo()
            .GetDeclaredMethod(nameof(LoadNavigation))!;

        private static void LoadNavigation(DbContext context, object entity, string navigationName, bool isCollection)
        {
            var entry = context.Entry(entity);
            if (isCollection)
                entry.Collection(navigationName).Load();
            else
                entry.Reference(navigationName).Load();
        }
    }

    private static IEnumerable<TResult> Query<TResult>(
        QueryContext queryContext,
        IEnumerable<ValueBuffer> valueBuffers,
        Func<QueryContext, ValueBuffer, TResult> shaper)
    {
        // Correctness-first: execute client-side for now.
        // TODO: push query operators (filters/limits/projections) into DB2-native execution.
        foreach (var valueBuffer in valueBuffers)
        {
            yield return shaper(queryContext, valueBuffer);
        }
    }

    private static IEnumerable<ValueBuffer> Table(QueryContext queryContext, Db2QueryExpression queryExpression)
    {
        var dbContext = queryContext.Context;

        var entityType = queryExpression.EntityType;
        var tableName = entityType.GetTableName() ?? entityType.ClrType.Name;

        var options = dbContext.GetService<IDbContextOptions>();
        var extension = options.FindExtension<MimironDb2OptionsExtension>();

        var wowVersion = extension?.WowVersion;
        if (string.IsNullOrWhiteSpace(wowVersion))
            throw new InvalidOperationException("MimironDb2 WOW_VERSION is not configured. Call UseMimironDb2(o => o.WithWowVersion(...)).");

        var db2StreamProvider = dbContext.GetService<IDb2StreamProvider>();
        var dbdProvider = dbContext.GetService<IDbdProvider>();

        // Prefer DI-provided format, but default to WDC5 for now.
        var format = dbContext.GetService<IDb2Format>() ?? new Wdc5Format();

        var schemaMapper = new SchemaMapper(dbdProvider, wowVersion);
        var schema = schemaMapper.GetSchema(tableName);

        // The file must remain open while enumerating; IDb2File owns the stream lifetime.
        var stream = db2StreamProvider.OpenDb2Stream(tableName);
        var file = format.OpenFile(stream);

        if (file is not IDb2File<RowHandle> typedFile)
            throw new NotSupportedException($"DB2 file for '{tableName}' does not implement IDb2File<RowHandle> (RowType={file.RowType.FullName}).");

        var layout = format.GetLayout(file);
        if (!schema.AllowsAnyLayoutHash && schema.AllowedLayoutHashes is { Count: > 0 } allowed && !allowed.Contains(layout.LayoutHash))
        {
            throw new InvalidDataException(
                $"DB2 layout hash 0x{layout.LayoutHash:X8} is not allowed for {tableName}. Allowed: {string.Join(", ", allowed.Select(h => $"0x{h:X8}"))}.");
        }

        var properties = entityType.GetProperties().Where(static p => p.PropertyInfo is not null && !p.IsShadowProperty()).ToArray();
        var maxIndex = properties.Length == 0 ? -1 : properties.Max(static p => p.GetIndex());
        var values = maxIndex >= 0 ? new object?[maxIndex + 1] : Array.Empty<object?>();

        foreach (var handle in typedFile.EnumerateRowHandles())
        {
            Array.Clear(values);

            foreach (var property in properties)
            {
                if (property.PropertyInfo is not { } propInfo)
                    continue;

                var columnName = property.GetColumnName() ?? propInfo.Name;
                if (!schema.TryGetFieldCaseInsensitive(columnName, out var fieldSchema))
                    continue;

                values[property.GetIndex()] = ReadFieldBoxed(typedFile, handle, property.ClrType, fieldSchema.ColumnStartIndex);
            }

            yield return new ValueBuffer(values);
        }
    }

    private static object? ReadFieldBoxed(IDb2File<RowHandle> file, RowHandle handle, Type clrType, int fieldIndex)
    {
        ArgumentNullException.ThrowIfNull(file);
        ArgumentNullException.ThrowIfNull(clrType);

        var underlying = Nullable.GetUnderlyingType(clrType);
        if (underlying is not null)
        {
            var value = ReadFieldBoxed(file, handle, underlying, fieldIndex);
            if (value is null)
                return null;

            return Activator.CreateInstance(clrType, value);
        }

        // Use IDb2File.ReadField<T> with reflection to support scalar, enum, string, and array types.
        var method = typeof(IDb2File).GetMethod(nameof(IDb2File.ReadField))!;
        var generic = method.MakeGenericMethod(clrType);
        return generic.Invoke(file, [handle, fieldIndex]);
    }
}
