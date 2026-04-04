using System.Collections;
using System.Linq.Expressions;
using System.Reflection;

using MimironSQL.EntityFrameworkCore.Model;
using MimironSQL.EntityFrameworkCore.Schema;
using MimironSQL.Formats;

namespace MimironSQL.EntityFrameworkCore.Query.Internal;

internal sealed class Db2EntityMaterializer<TEntity>
    where TEntity : class
{
    private readonly Func<TEntity> _factory;
    private readonly Action<TEntity, IDb2File, RowHandle> _apply;

    public Db2EntityMaterializer(Db2ModelBinding model, Db2EntityType entityType, IDb2EntityFactory? entityFactory = null)
    {
        ArgumentNullException.ThrowIfNull(model);
        ArgumentNullException.ThrowIfNull(entityType);

        entityFactory ??= new DefaultDb2EntityFactory();
        _factory = entityFactory.Create<TEntity>;
        _apply = Db2EntityMaterializerCache.GetOrCompile<TEntity>(model.EfModel, entityType);
    }

    public TEntity Materialize(IDb2File file, RowHandle handle)
    {
        var entity = _factory();
        _apply(entity, file, handle);
        return entity;
    }

    internal static Action<TEntity, IDb2File, RowHandle> CompileApply(Db2EntityType entityType)
    {
        var entity = Expression.Parameter(typeof(TEntity), "entity");
        var file = Expression.Parameter(typeof(IDb2File), "file");
        var handle = Expression.Parameter(typeof(RowHandle), "handle");

        var assigns = new List<Expression>();

        var properties = typeof(TEntity)
            .GetProperties(BindingFlags.Instance | BindingFlags.Public)
            .Where(p => p.GetMethod is { IsPublic: true });

        foreach (var property in properties.Where(p => entityType.TryResolveFieldSchema(p, out _)))
        {
            entityType.TryResolveFieldSchema(property, out var field);

            var memberType = property.PropertyType;

            if (memberType.IsArray)
            {
                var elementType = memberType.GetElementType()!;
                if (elementType == typeof(string))
                {
                    continue;
                }

                if (TryCreateArrayAssign(entity, file, handle, property, field, elementType, out var assign))
                {
                    assigns.Add(assign);
                }

                continue;
            }

            if (TryCreateSchemaArrayCollectionAssign(entity, file, handle, property, field, memberType, out var collectionAssign))
            {
                assigns.Add(collectionAssign);
                continue;
            }

            // If the schema describes an array-valued field, do not fall back to scalar binding for
            // collection-like CLR members we don't explicitly support (only T[] and ICollection<T> are supported).
            if (field.ElementCount > 1 && typeof(IEnumerable).IsAssignableFrom(memberType))
            {
                continue;
            }

            if (TryCreateScalarAssign(entity, file, handle, property, field, memberType, out var scalarAssign))
            {
                assigns.Add(scalarAssign);
            }
        }

        Expression body = assigns.Count == 0 ? Expression.Empty() : Expression.Block(assigns);
        return Expression.Lambda<Action<TEntity, IDb2File, RowHandle>>(body, entity, file, handle).Compile();
    }

    private static bool TryCreateSchemaArrayCollectionAssign(
        ParameterExpression entity,
        ParameterExpression file,
        ParameterExpression handle,
        PropertyInfo property,
        Db2FieldSchema field,
        Type memberType,
        out Expression assign)
    {
        if (!memberType.IsGenericType)
        {
            assign = null!;
            return false;
        }

        if (field.ElementCount <= 1)
        {
            assign = null!;
            return false;
        }

        var genericDefinition = memberType.GetGenericTypeDefinition();
        if (genericDefinition != typeof(ICollection<>))
        {
            assign = null!;
            return false;
        }

        var elementType = memberType.GetGenericArguments()[0];
        if (elementType == typeof(string))
        {
            assign = null!;
            return false;
        }

        if (!elementType.IsPrimitive && elementType != typeof(float) && elementType != typeof(double))
        {
            assign = null!;
            return false;
        }

        var arrayType = elementType.MakeArrayType();
        assign = CreateSchemaArrayCollectionAssign(entity, file, handle, property, memberType, arrayType, field.ColumnStartIndex);
        return true;
    }

    private static Expression CreateSchemaArrayCollectionAssign(
        ParameterExpression entity,
        ParameterExpression file,
        ParameterExpression handle,
        PropertyInfo property,
        Type memberType,
        Type arrayType,
        int fieldIndex)
    {
        var readArray = BuildReadExpression(file, handle, fieldIndex, arrayType);

        switch (property.SetMethod)
        {
            case { IsPublic: true }:
                {
                    var memberAccess = Expression.Property(entity, property);
                    return Expression.Assign(memberAccess, Expression.Convert(readArray, memberType));
                }

            default:
                throw new NotSupportedException($"Property '{property.Name}' must be writable for materialization.");
        }
    }

    private static bool TryCreateScalarAssign(
        ParameterExpression entity,
        ParameterExpression file,
        ParameterExpression handle,
        PropertyInfo property,
        Db2FieldSchema field,
        Type memberType,
        out Expression assign)
    {
        // EF primary keys for DB2 entities map to the DB2 row ID. Even when DBD declares an ID field,
        // the most reliable source for EF tracking is the RowHandle.RowId.
        if (field.IsId && TryCreateRowIdAssign(entity, handle, property, memberType, out assign))
        {
            return true;
        }

        if (memberType == typeof(string) && field.IsVirtual)
        {
            assign = null!;
            return false;
        }

        assign = CreateAssign(entity, file, handle, property, memberType, field.ColumnStartIndex);
        return true;
    }

    private static bool TryCreateRowIdAssign(
        ParameterExpression entity,
        ParameterExpression handle,
        PropertyInfo property,
        Type memberType,
        out Expression assign)
    {
        assign = null!;

        var rowId = Expression.Property(handle, nameof(RowHandle.RowId));

        Expression value = memberType switch
        {
            _ when memberType == typeof(int) => rowId,
            _ when memberType == typeof(int?) => Expression.Convert(rowId, typeof(int?)),
            _ when memberType == typeof(uint) => Expression.Convert(rowId, typeof(uint)),
            _ when memberType == typeof(uint?) => Expression.Convert(rowId, typeof(uint?)),
            _ => null!,
        };

        if (value is null)
        {
            return false;
        }

        switch (property.SetMethod)
        {
            case { IsPublic: true }:
                assign = Expression.Assign(Expression.Property(entity, property), value);
                return true;

            default:
                throw new NotSupportedException($"Property '{property.Name}' must be writable for materialization.");
        }
    }

    private static bool TryCreateArrayAssign(
        ParameterExpression entity,
        ParameterExpression file,
        ParameterExpression handle,
        PropertyInfo property,
        Db2FieldSchema field,
        Type elementType,
        out Expression assign)
    {
        if (!elementType.IsPrimitive && elementType != typeof(float) && elementType != typeof(double))
        {
            assign = null!;
            return false;
        }

        var arrayType = elementType.MakeArrayType();
        assign = CreateAssign(entity, file, handle, property, arrayType, field.ColumnStartIndex);
        return true;
    }

    private static Expression CreateAssign(
        ParameterExpression entity,
        ParameterExpression file,
        ParameterExpression handle,
        PropertyInfo property,
        Type memberType,
        int fieldIndex)
    {
        var read = BuildReadExpression(file, handle, fieldIndex, memberType);

        switch (property.SetMethod)
        {
            case { IsPublic: true }:
                {
                    var memberAccess = Expression.Property(entity, property);
                    return Expression.Assign(memberAccess, read);
                }

            default:
                throw new NotSupportedException($"Property '{property.Name}' must be writable for materialization.");
        }
    }

    private static MethodCallExpression BuildReadExpression(Expression fileExpression, Expression handleExpression, int fieldIndex, Type targetType)
    {
        var readFieldMethod = typeof(IDb2File)
            .GetMethod(nameof(IDb2File.ReadField), BindingFlags.Instance | BindingFlags.Public)!
            .MakeGenericMethod(targetType);

        return Expression.Call(fileExpression, readFieldMethod, handleExpression, Expression.Constant(fieldIndex));
    }
}
