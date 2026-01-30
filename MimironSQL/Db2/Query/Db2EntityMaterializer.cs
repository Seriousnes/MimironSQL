using MimironSQL.Db2.Schema;
using MimironSQL.Formats;
using MimironSQL.Extensions;

using System.Linq.Expressions;
using System.Reflection;

namespace MimironSQL.Db2.Query;

internal sealed class Db2EntityMaterializer<TEntity, TRow>
    where TRow : struct, IDb2Row
{
    private readonly Func<TEntity> _factory;
    private readonly IReadOnlyList<IBinding> _bindings;

    public Db2EntityMaterializer(Db2TableSchema schema)
    {
        ArgumentNullException.ThrowIfNull(schema);

        _factory = CreateFactory();
        _bindings = CreateBindings(schema);
    }

    public TEntity Materialize(TRow row)
    {
        var entity = _factory();
        foreach (var b in _bindings)
            b.Apply(entity, row);

        return entity;
    }

    private static Func<TEntity> CreateFactory()
    {
        var ctor = typeof(TEntity).GetConstructor(Type.EmptyTypes) ?? throw new NotSupportedException($"Entity type {typeof(TEntity).FullName} must have a public parameterless constructor for reflection-based materialization.");
        var body = Expression.New(ctor);
        return Expression.Lambda<Func<TEntity>>(body).Compile();
    }

    private static IReadOnlyList<IBinding> CreateBindings(Db2TableSchema schema)
    {
        var fields = schema.Fields.ToDictionary(f => f.Name, StringComparer.OrdinalIgnoreCase);

        var memberBindings = new List<IBinding>();

        var members = typeof(TEntity)
            .GetMembers(BindingFlags.Instance | BindingFlags.Public)
            .Where(m => m is PropertyInfo or FieldInfo);

        foreach (var member in members)
        {
            var name = member.Name;
            if (!fields.TryGetValue(name, out var field))
                continue;

            var memberType = member switch
            {
                PropertyInfo p => p.PropertyType,
                FieldInfo f => f.FieldType,
                _ => throw new InvalidOperationException($"Unexpected member type: {member.GetType().FullName}"),
            };

            if (memberType.IsArray)
            {
                var elementType = memberType.GetElementType()!;
                if (elementType == typeof(string))
                    continue;

                if (TryCreateArrayBinding(member, field, elementType, out var binding))
                    memberBindings.Add(binding);

                continue;
            }

            if (TryCreateSchemaArrayCollectionBinding(member, field, memberType, out var collectionBinding))
            {
                memberBindings.Add(collectionBinding);
                continue;
            }

            if (TryCreateScalarBinding(member, field, memberType, out var scalarBinding))
                memberBindings.Add(scalarBinding);
        }

        return memberBindings;
    }

    private static bool TryCreateSchemaArrayCollectionBinding(MemberInfo member, Db2FieldSchema field, Type memberType, out IBinding binding)
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
        binding = CreateSchemaArrayCollectionBinding(member, memberType, arrayType, field.ColumnStartIndex);
        return true;
    }

    private static IBinding CreateSchemaArrayCollectionBinding(MemberInfo member, Type memberType, Type arrayType, int fieldIndex)
    {
        var entity = Expression.Parameter(typeof(TEntity), "entity");
        var row = Expression.Parameter(typeof(TRow), "row");

        Expression memberAccess;
        if (member is PropertyInfo p)
        {
            if (!p.CanWrite)
                throw new NotSupportedException($"Property '{p.Name}' must be writable for materialization.");

            memberAccess = Expression.Property(entity, p);
        }
        else if (member is FieldInfo f)
        {
            memberAccess = Expression.Field(entity, f);
        }
        else
        {
            throw new InvalidOperationException($"Unexpected member type: {member.GetType().FullName}");
        }

        var readArray = BuildReadExpression(row, fieldIndex, arrayType);
        var assign = Expression.Assign(memberAccess, Expression.Convert(readArray, memberType));
        var apply = Expression.Lambda<Action<TEntity, TRow>>(assign, entity, row).Compile();
        return new Binding(apply);
    }

    private static bool TryCreateScalarBinding(MemberInfo member, Db2FieldSchema field, Type memberType, out IBinding binding)
    {
        if (memberType == typeof(string) && field.IsVirtual)
        {
            binding = null!;
            return false;
        }

        binding = CreateBinding(member, memberType, field.ColumnStartIndex);
        return true;
    }

    private static bool TryCreateArrayBinding(MemberInfo member, Db2FieldSchema field, Type elementType, out IBinding binding)
    {
        if (!elementType.IsPrimitive && elementType != typeof(float) && elementType != typeof(double))
        {
            binding = null!;
            return false;
        }

        var arrayType = elementType.MakeArrayType();
        binding = CreateBinding(member, arrayType, field.ColumnStartIndex);
        return true;
    }

    private static IBinding CreateBinding(MemberInfo member, Type memberType, int fieldIndex)
    {
        var entity = Expression.Parameter(typeof(TEntity), "entity");
        var row = Expression.Parameter(typeof(TRow), "row");

        Expression memberAccess;
        if (member is PropertyInfo p)
        {
            if (!p.CanWrite)
                throw new NotSupportedException($"Property '{p.Name}' must be writable for materialization.");

            memberAccess = Expression.Property(entity, p);
        }
        else if (member is FieldInfo f)
        {
            memberAccess = Expression.Field(entity, f);
        }
        else
        {
            throw new InvalidOperationException($"Unexpected member type: {member.GetType().FullName}");
        }

        var read = BuildReadExpression(row, fieldIndex, memberType);
        var assign = Expression.Assign(memberAccess, read);
        var apply = Expression.Lambda<Action<TEntity, TRow>>(assign, entity, row).Compile();
        return new Binding(apply);
    }

    private static Expression BuildReadExpression(Expression rowExpression, int fieldIndex, Type targetType)
    {
        var getMethod = typeof(TRow)
            .GetMethod(nameof(IDb2Row.Get), BindingFlags.Instance | BindingFlags.Public, [typeof(int)])!
            .MakeGenericMethod(targetType);

        return Expression.Call(rowExpression, getMethod, Expression.Constant(fieldIndex));
    }

    private interface IBinding
    {
        void Apply(TEntity entity, TRow row);
    }

    private sealed record Binding(Action<TEntity, TRow> ApplyAction) : IBinding
    {
        public void Apply(TEntity entity, TRow row)
            => ApplyAction(entity, row);
    }
}
