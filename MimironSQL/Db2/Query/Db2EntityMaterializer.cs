using MimironSQL.Db2.Model;
using MimironSQL.Db2.Schema;
using MimironSQL.Formats;

using System.Linq.Expressions;
using System.Reflection;

namespace MimironSQL.Db2.Query;

internal sealed class Db2EntityMaterializer<TEntity, TRow>
    where TRow : struct
{
    private readonly Func<TEntity> _factory;
    private readonly IReadOnlyList<Binding> _bindings;

    public Db2EntityMaterializer(Db2EntityType entityType)
    {
        ArgumentNullException.ThrowIfNull(entityType);

        _factory = CreateFactory();
        _bindings = CreateBindings(entityType);
    }

    public TEntity Materialize(IDb2File<TRow> file, RowHandle handle)
    {
        var entity = _factory();
        foreach (var b in _bindings)
            b.Apply(entity, file, handle);

        return entity;
    }

    private static Func<TEntity> CreateFactory()
    {
        var ctor = typeof(TEntity).GetConstructor(Type.EmptyTypes) ?? throw new NotSupportedException($"Entity type {typeof(TEntity).FullName} must have a public parameterless constructor for reflection-based materialization.");
        var body = Expression.New(ctor);
        return Expression.Lambda<Func<TEntity>>(body).Compile();
    }

    private static IReadOnlyList<Binding> CreateBindings(Db2EntityType entityType)
    {
        var memberBindings = new List<Binding>();

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
                    continue;

                if (TryCreateArrayBinding(property, field, elementType, out var binding))
                    memberBindings.Add(binding);

                continue;
            }

            if (TryCreateSchemaArrayCollectionBinding(property, field, memberType, out var collectionBinding))
            {
                memberBindings.Add(collectionBinding);
                continue;
            }

            if (TryCreateScalarBinding(property, field, memberType, out var scalarBinding))
                memberBindings.Add(scalarBinding);
        }

        return [.. memberBindings];
    }

    private static bool TryCreateSchemaArrayCollectionBinding(PropertyInfo property, Db2FieldSchema field, Type memberType, out Binding binding)
    {
        if (!memberType.IsGenericType)
        {
            binding = null!;
            return false;
        }

        if (field.ElementCount <= 1)
        {
            binding = null!;
            return false;
        }

        var genericDefinition = memberType.GetGenericTypeDefinition();
        if (genericDefinition != typeof(ICollection<>)
            && genericDefinition != typeof(IList<>)
            && genericDefinition != typeof(IEnumerable<>)
            && genericDefinition != typeof(IReadOnlyCollection<>)
            && genericDefinition != typeof(IReadOnlyList<>))
        {
            binding = null!;
            return false;
        }

        var elementType = memberType.GetGenericArguments()[0];
        if (elementType == typeof(string))
        {
            binding = null!;
            return false;
        }

        if (!elementType.IsPrimitive && elementType != typeof(float) && elementType != typeof(double))
        {
            binding = null!;
            return false;
        }

        var arrayType = elementType.MakeArrayType();
        binding = CreateSchemaArrayCollectionBinding(property, memberType, arrayType, field.ColumnStartIndex);
        return true;
    }

    private static Binding CreateSchemaArrayCollectionBinding(PropertyInfo property, Type memberType, Type arrayType, int fieldIndex)
    {
        var entity = Expression.Parameter(typeof(TEntity), "entity");
        var file = Expression.Parameter(typeof(IDb2File<TRow>), "file");
        var handle = Expression.Parameter(typeof(RowHandle), "handle");

        var readArray = BuildReadExpression(file, handle, fieldIndex, arrayType);

        switch (property.SetMethod)
        {
            case { IsPublic: true }:
                {
                    var memberAccess = Expression.Property(entity, property);
                    var assign = Expression.Assign(memberAccess, Expression.Convert(readArray, memberType));
                    var apply = Expression.Lambda<Action<TEntity, IDb2File<TRow>, RowHandle>>(assign, entity, file, handle).Compile();
                    return new Binding(apply);
                }

            default:
                throw new NotSupportedException($"Property '{property.Name}' must be writable for materialization.");
        }
    }

    private static bool TryCreateScalarBinding(PropertyInfo property, Db2FieldSchema field, Type memberType, out Binding binding)
    {
        if (memberType == typeof(string) && field.IsVirtual)
        {
            binding = null!;
            return false;
        }

        binding = CreateBinding(property, memberType, field.ColumnStartIndex);
        return true;
    }

    private static bool TryCreateArrayBinding(PropertyInfo property, Db2FieldSchema field, Type elementType, out Binding binding)
    {
        if (!elementType.IsPrimitive && elementType != typeof(float) && elementType != typeof(double))
        {
            binding = null!;
            return false;
        }

        var arrayType = elementType.MakeArrayType();
        binding = CreateBinding(property, arrayType, field.ColumnStartIndex);
        return true;
    }

    private static Binding CreateBinding(PropertyInfo property, Type memberType, int fieldIndex)
    {
        var entity = Expression.Parameter(typeof(TEntity), "entity");
        var file = Expression.Parameter(typeof(IDb2File<TRow>), "file");
        var handle = Expression.Parameter(typeof(RowHandle), "handle");

        var read = BuildReadExpression(file, handle, fieldIndex, memberType);

        switch (property.SetMethod)
        {
            case { IsPublic: true }:
                {
                    var memberAccess = Expression.Property(entity, property);
                    var assign = Expression.Assign(memberAccess, read);
                    var apply = Expression.Lambda<Action<TEntity, IDb2File<TRow>, RowHandle>>(assign, entity, file, handle).Compile();
                    return new Binding(apply);
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

    private sealed record Binding(Action<TEntity, IDb2File<TRow>, RowHandle> ApplyAction)
    {
        public void Apply(TEntity entity, IDb2File<TRow> file, RowHandle handle)
            => ApplyAction(entity, file, handle);
    }
}
