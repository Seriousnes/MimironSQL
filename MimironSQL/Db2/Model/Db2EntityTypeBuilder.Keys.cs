using System.Linq.Expressions;
using System.Reflection;

namespace MimironSQL.Db2.Model;

public sealed partial class Db2EntityTypeBuilder<T>
{
    public Db2EntityTypeBuilder<T> HasKey<TKey>(Expression<Func<T, TKey>> key)
    {
        ArgumentNullException.ThrowIfNull(key);

        if (key.Parameters.Count != 1)
            throw new NotSupportedException("Key selector must have exactly one parameter.");

        var body = key.Body;
        if (body is UnaryExpression { NodeType: ExpressionType.Convert or ExpressionType.ConvertChecked } u)
            body = u.Operand;

        if (body is not MemberExpression { Member: PropertyInfo or FieldInfo } member)
            throw new NotSupportedException("Key selector only supports simple member access (e.g., x => x.Id). ");

        if (member.Expression != key.Parameters[0])
            throw new NotSupportedException("Key selector only supports direct member access on the root entity parameter.");

        Metadata.PrimaryKeyMember = member.Member;
        Metadata.PrimaryKeyWasConfigured = true;
        return this;
    }
}
