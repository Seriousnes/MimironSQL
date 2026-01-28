using MimironSQL.Db2.Schema;
using MimironSQL.Db2.Wdc5;
using MimironSQL.Extensions;

using System.Linq.Expressions;
using System.Reflection;

namespace MimironSQL.Db2.Query;

internal sealed class Db2EntityMaterializer<TEntity>
{
    private readonly Func<TEntity> _factory;
    private readonly IReadOnlyList<Binding> _bindings;

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
            b.Setter(entity, b.Getter(row));

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

    private static IReadOnlyList<Binding> CreateBindings(Db2TableSchema schema)
    {
        var fields = schema.Fields.ToDictionary(f => f.Name, StringComparer.OrdinalIgnoreCase);

        var memberBindings = new List<Binding>();

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

                if (!TryCreateArrayGetter(field, elementType, out var getter))
                    continue;

                if (TryCreateSetter(member, memberType, out var value) && value is { } setter)
                    memberBindings.Add(new Binding(getter, setter));

                continue;
            }

            if (!TryCreateScalarGetter(field, memberType, out var scalarGetter))
                continue;

            if (TryCreateSetter(member, memberType, out var scalarValue) && scalarValue is { } scalarSetter)
                memberBindings.Add(new Binding(scalarGetter, scalarSetter));
        }

        return memberBindings;
    }

    private static bool TryCreateScalarGetter(Db2FieldSchema field, Type memberType, out Func<Wdc5Row, object?> getter)
    {
        var accessor = new Db2FieldAccessor(field);

        var method = typeof(Db2RowValue).GetMethod(nameof(Db2RowValue.Read), BindingFlags.Public | BindingFlags.Static);
        var generic = method!.MakeGenericMethod(memberType.UnwrapNullable());

        getter = row => generic.Invoke(null, [row, accessor]);
        return true;
    }

    private static bool TryCreateArrayGetter(Db2FieldSchema field, Type elementType, out Func<Wdc5Row, object?> getter)
    {
        var accessor = new Db2FieldAccessor(field);

        if (!elementType.IsPrimitive && elementType != typeof(float) && elementType != typeof(double))
        {
            getter = _ => null;
            return false;
        }

        var method = typeof(Db2RowValue).GetMethod(nameof(Db2RowValue.ReadArray), BindingFlags.Public | BindingFlags.Static);
        var generic = method!.MakeGenericMethod(elementType);

        getter = row => generic.Invoke(null, [row, accessor]);
        return true;
    }

    private static bool TryCreateSetter(MemberInfo member, Type memberType, out Action<TEntity, object?>? setter)
    {
        var entity = Expression.Parameter(typeof(TEntity), "entity");
        var value = Expression.Parameter(typeof(object), "value");

        Expression memberAccess;
        if (member is PropertyInfo p)
        {
            if (!p.CanWrite)
            {
                setter = null;
                return false;
            }

            memberAccess = Expression.Property(entity, p);
        }
        else if (member is FieldInfo f)
        {
            memberAccess = Expression.Field(entity, f);
        }
        else
        {
            setter = null;
            return false;
        }

        var targetType = memberType.UnwrapNullable();
        var converted = Expression.Convert(value, targetType);

        Expression assign = Expression.Assign(memberAccess, converted);
        if (memberType.IsNullable())
        {
            var nullableTarget = Expression.Convert(value, memberType);
            assign = Expression.Assign(memberAccess, nullableTarget);
        }

        setter = Expression.Lambda<Action<TEntity, object?>>(assign, entity, value).Compile();
        return true;
    }

    private sealed record Binding(Func<Wdc5Row, object?> Getter, Action<TEntity, object?> Setter);
}
