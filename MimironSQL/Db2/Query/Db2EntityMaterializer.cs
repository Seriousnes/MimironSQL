using MimironSQL.Db2.Schema;
using MimironSQL.Db2.Wdc5;
using MimironSQL.Extensions;

using System.Linq.Expressions;
using System.Reflection;

namespace MimironSQL.Db2.Query;

internal sealed class Db2EntityMaterializer<TEntity>
{
    private readonly Func<TEntity> _factory;
    private readonly IReadOnlyList<IBinding> _bindings;

    public Db2EntityMaterializer(Db2TableSchema schema)
    {
        ArgumentNullException.ThrowIfNull(schema);

        _factory = CreateFactory();
        _bindings = CreateBindings(schema);
    }

    public TEntity Materialize(Wdc5Row row)
    {
        var entity = _factory();
        foreach (var b in _bindings)
            b.Apply(entity, row);

        return entity;
    }

    private static Func<TEntity> CreateFactory()
    {
        var ctor = typeof(TEntity).GetConstructor(Type.EmptyTypes);
        if (ctor is null)
            throw new NotSupportedException($"Entity type {typeof(TEntity).FullName} must have a public parameterless constructor for reflection-based materialization.");

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

            if (TryCreateScalarBinding(member, field, memberType, out var scalarBinding))
                memberBindings.Add(scalarBinding);
        }

        return memberBindings;
    }

    private static bool TryCreateScalarBinding(MemberInfo member, Db2FieldSchema field, Type memberType, out IBinding binding)
    {
        if (memberType == typeof(string) && field.IsVirtual)
        {
            binding = null!;
            return false;
        }

        binding = CreateBinding(member, memberType, new Db2FieldAccessor(field));
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
        binding = CreateBinding(member, arrayType, new Db2FieldAccessor(field));
        return true;
    }

    private static IBinding CreateBinding(MemberInfo member, Type memberType, Db2FieldAccessor accessor)
    {
        var entity = Expression.Parameter(typeof(TEntity), "entity");
        var row = Expression.Parameter(typeof(Wdc5Row), "row");

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

        var read = BuildReadExpression(row, accessor, memberType);
        var assign = Expression.Assign(memberAccess, read);
        var apply = Expression.Lambda<Action<TEntity, Wdc5Row>>(assign, entity, row).Compile();
        return new Binding(apply);
    }

    private static Expression BuildReadExpression(Expression rowExpression, Db2FieldAccessor accessor, Type targetType)
    {
        if (targetType.IsArray)
        {
            var elementType = targetType.GetElementType()!;
            if (elementType == typeof(string))
                throw new NotSupportedException("String arrays are not supported.");

            if (!elementType.IsPrimitive && elementType != typeof(float) && elementType != typeof(double))
                throw new NotSupportedException($"Unsupported array element type {elementType.FullName}.");

            var method = typeof(Db2RowValue).GetMethod(nameof(Db2RowValue.ReadArray), BindingFlags.Public | BindingFlags.Static)!;
            var generic = method.MakeGenericMethod(elementType);
            return Expression.Call(generic, rowExpression, Expression.Constant(accessor));
        }

        if (targetType == typeof(string))
        {
            var readString = typeof(Db2RowValue).GetMethod(nameof(Db2RowValue.ReadString), BindingFlags.Public | BindingFlags.Static)!;
            return Expression.Call(readString, rowExpression, Expression.Constant(accessor));
        }

        var nonNullableTarget = targetType.UnwrapNullable();

        if (nonNullableTarget.IsEnum)
        {
            var underlying = Enum.GetUnderlyingType(nonNullableTarget);
            var readUnderlying = Expression.Call(underlying.GetReadMethod(), rowExpression, Expression.Constant(accessor));
            var enumValue = Expression.Convert(readUnderlying, nonNullableTarget);
            return targetType.IsNullable() ? Expression.Convert(enumValue, targetType) : enumValue;
        }

        var read = Expression.Call(nonNullableTarget.GetReadMethod(), rowExpression, Expression.Constant(accessor));
        return targetType.IsNullable() ? Expression.Convert(read, targetType) : read;
    }

    private interface IBinding
    {
        void Apply(TEntity entity, Wdc5Row row);
    }

    private sealed record Binding(Action<TEntity, Wdc5Row> ApplyAction) : IBinding
    {
        public void Apply(TEntity entity, Wdc5Row row)
            => ApplyAction(entity, row);
    }
}
