using System.Diagnostics.CodeAnalysis;
using System.Linq.Expressions;
using System.Reflection;

using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Query;

using MimironSQL.EntityFrameworkCore.Db2.Query.Expressions;
using MimironSQL.EntityFrameworkCore.Db2.Schema;

namespace MimironSQL.EntityFrameworkCore.Db2.Query;

/// <summary>
/// Translates CLR predicate expressions (from Where lambda bodies) into <see cref="Db2FilterExpression"/> trees.
/// Returns <c>null</c> for expressions that cannot be pushed down to the DB2 file reader.
/// </summary>
internal sealed class Db2ExpressionTranslator
{
    /// <summary>
    /// Translates a predicate lambda into a <see cref="Db2FilterExpression"/>.
    /// </summary>
    /// <param name="predicate">The lambda expression representing the predicate (e.g., <c>x => x.Id == 1</c>).</param>
    /// <param name="entityType">The EF Core entity type being filtered.</param>
    /// <returns>A <see cref="Db2FilterExpression"/> tree, or <c>null</c> if the predicate cannot be translated.</returns>
    public Db2FilterExpression? Translate(LambdaExpression predicate, IEntityType entityType)
    {
        var parameter = predicate.Parameters[0];
        return TranslateExpression(predicate.Body, entityType, parameter);
    }

    /// <summary>
    /// Translates a predicate lambda for a joined query into a <see cref="Db2FilterExpression"/>.
    /// Handles predicates that access both outer and inner entity properties.
    /// </summary>
    public Db2FilterExpression? TranslateJoined(
        LambdaExpression predicate,
        IEntityType outerEntityType,
        IEntityType innerEntityType,
        string innerTableName,
        Expression shaperExpression)
    {
        var parameter = predicate.Parameters[0];

        // Create a context that understands how to resolve both outer and inner field accesses
        var context = new JoinedTranslationContext(
            parameter,
            outerEntityType,
            innerEntityType,
            innerTableName,
            shaperExpression);

        return TranslateJoinedExpression(predicate.Body, context);
    }

    private Db2FilterExpression? TranslateJoinedExpression(Expression expression, JoinedTranslationContext context)
    {
        return expression switch
        {
            BinaryExpression binary => TranslateJoinedBinary(binary, context),
            UnaryExpression { NodeType: ExpressionType.Not } unary => TranslateJoinedNot(unary, context),
            MethodCallExpression methodCall => TranslateJoinedMethodCall(methodCall, context),
            _ => null, // Untranslatable
        };
    }

    private Db2FilterExpression? TranslateJoinedBinary(BinaryExpression binary, JoinedTranslationContext context)
    {
        // Logical operators
        if (binary.NodeType is ExpressionType.AndAlso)
        {
            var left = TranslateJoinedExpression(binary.Left, context);
            var right = TranslateJoinedExpression(binary.Right, context);
            if (left is null || right is null) return null;
            return new Db2AndFilterExpression(left, right);
        }

        if (binary.NodeType is ExpressionType.OrElse)
        {
            var left = TranslateJoinedExpression(binary.Left, context);
            var right = TranslateJoinedExpression(binary.Right, context);
            if (left is null || right is null) return null;
            return new Db2OrFilterExpression(left, right);
        }

        // Comparison operators
        var comparisonKind = binary.NodeType switch
        {
            ExpressionType.Equal => Db2ComparisonKind.Equal,
            ExpressionType.NotEqual => Db2ComparisonKind.NotEqual,
            ExpressionType.GreaterThan => Db2ComparisonKind.GreaterThan,
            ExpressionType.GreaterThanOrEqual => Db2ComparisonKind.GreaterThanOrEqual,
            ExpressionType.LessThan => Db2ComparisonKind.LessThan,
            ExpressionType.LessThanOrEqual => Db2ComparisonKind.LessThanOrEqual,
            _ => (Db2ComparisonKind?)null,
        };

        if (comparisonKind is null) return null;

        // Special case: entity null check (e.g., m.Inner != null or m.Outer != null)
        // This is used by EF Core to filter based on whether a join found a match
        if (TryExtractJoinedEntityNullCheck(binary.Left, binary.Right, context, comparisonKind.Value, out var entityNullCheckFilter)
            || TryExtractJoinedEntityNullCheck(binary.Right, binary.Left, context, comparisonKind.Value, out entityNullCheckFilter))
        {
            return entityNullCheckFilter;
        }

        // Try to extract field and value from the joined expression
        if (TryExtractJoinedFieldAndValue(binary.Left, binary.Right, context, out var field, out var joinedField, out var value)
            || TryExtractJoinedFieldAndValue(binary.Right, binary.Left, context, out field, out joinedField, out value))
        {
            // Check for null comparison
            if (joinedField is not null)
            {
                if (value is null && comparisonKind is Db2ComparisonKind.Equal)
                    return new Db2JoinedNullCheckFilterExpression(joinedField, isNotNull: false);
                if (value is null && comparisonKind is Db2ComparisonKind.NotEqual)
                    return new Db2JoinedNullCheckFilterExpression(joinedField, isNotNull: true);
                return new Db2JoinedComparisonFilterExpression(joinedField, comparisonKind.Value, value);
            }

            if (field is not null)
            {
                if (value is null && comparisonKind is Db2ComparisonKind.Equal)
                    return new Db2NullCheckFilterExpression(field, isNotNull: false);
                if (value is null && comparisonKind is Db2ComparisonKind.NotEqual)
                    return new Db2NullCheckFilterExpression(field, isNotNull: true);
                return new Db2ComparisonFilterExpression(field, comparisonKind.Value, value);
            }
        }

        return null;
    }

    private Db2FilterExpression? TranslateJoinedNot(UnaryExpression unary, JoinedTranslationContext context)
    {
        var inner = TranslateJoinedExpression(unary.Operand, context);
        return inner is null ? null : new Db2NotFilterExpression(inner);
    }

    private Db2FilterExpression? TranslateJoinedMethodCall(MethodCallExpression methodCall, JoinedTranslationContext context)
    {
        // Handle string methods: Contains, StartsWith, EndsWith
        if (TryTranslateJoinedStringMatch(methodCall, context, out var stringFilter))
            return stringFilter;

        // Handle string.IsNullOrWhiteSpace(x)
        if (TryTranslateJoinedStringIsNullOrWhiteSpace(methodCall, context, out var nullOrWhiteSpaceFilter))
            return nullOrWhiteSpaceFilter;

        return null;
    }

    private bool TryTranslateJoinedStringMatch(
        MethodCallExpression methodCall,
        JoinedTranslationContext context,
        out Db2FilterExpression? filter)
    {
        filter = null;

        if (methodCall.Object is null || methodCall.Arguments.Count != 1)
            return false;

        var matchKind = methodCall.Method.Name switch
        {
            nameof(string.Contains) when methodCall.Method.DeclaringType == typeof(string) => Db2StringMatchKind.Contains,
            nameof(string.StartsWith) when methodCall.Method.DeclaringType == typeof(string) => Db2StringMatchKind.StartsWith,
            nameof(string.EndsWith) when methodCall.Method.DeclaringType == typeof(string) => Db2StringMatchKind.EndsWith,
            _ => (Db2StringMatchKind?)null,
        };

        if (matchKind is null) return false;

        if (!TryEvaluateConstant(methodCall.Arguments[0], out var pattern))
            return false;

        // Try outer entity field
        if (TryResolveJoinedFieldAccess(methodCall.Object, context, out var outerField, out var innerField))
        {
            if (innerField is not null)
            {
                switch (pattern)
                {
                    case string s:
                        filter = new Db2JoinedStringMatchFilterExpression(innerField, matchKind.Value, s);
                        return true;
                    case char c:
                        filter = new Db2JoinedStringMatchFilterExpression(innerField, matchKind.Value, c.ToString());
                        return true;
                    case Db2RuntimeParameter p:
                        filter = new Db2JoinedStringMatchFilterExpression(innerField, matchKind.Value, p.Name, isParameter: true);
                        return true;
                    default:
                        return false;
                }
            }
            if (outerField is not null)
            {
                switch (pattern)
                {
                    case string s:
                        filter = new Db2StringMatchFilterExpression(outerField, matchKind.Value, s);
                        return true;
                    case char c:
                        filter = new Db2StringMatchFilterExpression(outerField, matchKind.Value, c.ToString());
                        return true;
                    case Db2RuntimeParameter p:
                        filter = new Db2StringMatchFilterExpression(outerField, matchKind.Value, p.Name, isParameter: true);
                        return true;
                    default:
                        return false;
                }
            }
        }

        return false;
    }

    private bool TryTranslateJoinedStringIsNullOrWhiteSpace(
        MethodCallExpression methodCall,
        JoinedTranslationContext context,
        [NotNullWhen(true)] out Db2FilterExpression? filter)
    {
        filter = null;

        if (methodCall.Object is not null
            || methodCall.Method.DeclaringType != typeof(string)
            || methodCall.Method.Name != nameof(string.IsNullOrWhiteSpace)
            || methodCall.Arguments.Count != 1)
        {
            return false;
        }

        if (!TryResolveJoinedFieldAccess(methodCall.Arguments[0], context, out var outerField, out var innerField))
            return false;

        if (innerField is not null)
        {
            var isNull = new Db2JoinedNullCheckFilterExpression(innerField, isNotNull: false);
            var isEmpty = new Db2JoinedComparisonFilterExpression(innerField, Db2ComparisonKind.Equal, "");
            filter = new Db2OrFilterExpression(isNull, isEmpty);
            return true;
        }

        if (outerField is not null)
        {
            var isNull = new Db2NullCheckFilterExpression(outerField, isNotNull: false);
            var isEmpty = new Db2ComparisonFilterExpression(outerField, Db2ComparisonKind.Equal, "");
            filter = new Db2OrFilterExpression(isNull, isEmpty);
            return true;
        }

        return false;
    }

    /// <summary>
    /// Extracts an entity null check from a joined expression.
    /// Handles patterns like: transparentId.Inner != null or transparentId.Outer != null
    /// </summary>
    private static bool TryExtractJoinedEntityNullCheck(
        Expression candidateEntity,
        Expression candidateNull,
        JoinedTranslationContext context,
        Db2ComparisonKind comparisonKind,
        [NotNullWhen(true)] out Db2FilterExpression? filter)
    {
        filter = null;

        // Only handle equality/inequality comparisons
        if (comparisonKind is not (Db2ComparisonKind.Equal or Db2ComparisonKind.NotEqual))
        {
            return false;
        }

        // The null side must be a constant null
        if (candidateNull is not ConstantExpression { Value: null })
        {
            return false;
        }

        // Check if the entity side is accessing Inner or Outer on TransparentIdentifier
        // Note: TransparentIdentifier uses fields, not properties, so we need to check both
        string? memberName = null;
        if (candidateEntity is MemberExpression memberExpr)
        {
            memberName = memberExpr.Member.Name;
        }
        
        if (memberName is null)
        {
            return false;
        }

        // Check for Inner entity null check
        if (memberName == "Inner")
        {
            // Create a joined null check on the primary key of the inner entity
            // This effectively checks if the join found a match
            var pkProperty = context.InnerEntityType.FindPrimaryKey()?.Properties.FirstOrDefault();
            if (pkProperty is not null)
            {
                var columnName = pkProperty.GetColumnName() ?? pkProperty.Name;
                var field = new Db2FieldSchema(
                    columnName,
                    default,
                    ColumnStartIndex: -1,
                    ElementCount: 1,
                    IsVerified: false,
                    IsVirtual: false,
                    IsId: true,
                    IsRelation: false,
                    ReferencedTableName: null);

                var joinedField = new Db2JoinedFieldAccessExpression(
                    context.InnerTableName,
                    field,
                    fieldIndex: -1,
                    pkProperty.ClrType);

                // innerId != null means the join found a match
                // innerId == null means no match was found
                var isNotNull = comparisonKind == Db2ComparisonKind.NotEqual;
                filter = new Db2JoinedNullCheckFilterExpression(joinedField, isNotNull);
                return true;
            }
        }

        // Check for Outer entity null check (less common, but handle it)
        if (memberName == "Outer")
        {
            var pkProperty = context.OuterEntityType.FindPrimaryKey()?.Properties.FirstOrDefault();
            if (pkProperty is not null)
            {
                var columnName = pkProperty.GetColumnName() ?? pkProperty.Name;
                var field = new Db2FieldSchema(
                    columnName,
                    default,
                    ColumnStartIndex: -1,
                    ElementCount: 1,
                    IsVerified: false,
                    IsVirtual: false,
                    IsId: true,
                    IsRelation: false,
                    ReferencedTableName: null);

                var outerField = new Db2FieldAccessExpression(field, fieldIndex: -1, pkProperty.ClrType);
                var isNotNull = comparisonKind == Db2ComparisonKind.NotEqual;
                filter = new Db2NullCheckFilterExpression(outerField, isNotNull);
                return true;
            }
        }

        return false;
    }

    private bool TryExtractJoinedFieldAndValue(
        Expression candidateField,
        Expression candidateValue,
        JoinedTranslationContext context,
        out Db2FieldAccessExpression? outerField,
        out Db2JoinedFieldAccessExpression? innerField,
        out object? value)
    {
        outerField = null;
        innerField = null;
        value = null;

        if (TryResolveJoinedFieldAccess(candidateField, context, out outerField, out innerField)
            && TryEvaluateConstant(candidateValue, out value))
        {
            return true;
        }
        outerField = null;
        innerField = null;
        value = null;
        return false;
    }

    /// <summary>
    /// Resolves a field access expression in a joined query context.
    /// Can resolve both outer entity fields (e.g., transparentId.Outer.Id)
    /// and inner entity fields (e.g., transparentId.Inner.Directory).
    /// </summary>
    private bool TryResolveJoinedFieldAccess(
        Expression expression,
        JoinedTranslationContext context,
        out Db2FieldAccessExpression? outerField,
        out Db2JoinedFieldAccessExpression? innerField)
    {
        outerField = null;
        innerField = null;

        // Handle patterns like: transparentId.Inner.PropertyName
        // or: transparentId.Outer.PropertyName
        if (expression is MemberExpression { Member: PropertyInfo property } memberExpr)
        {
            // Check if this is accessing a property on Inner or Outer
            // Note: TransparentIdentifier uses fields (not properties) for Inner/Outer
            if (memberExpr.Expression is MemberExpression parentMember)
            {
                var parentMemberName = parentMember.Member.Name;
                
                // Check for Inner (related entity)
                if (parentMemberName == "Inner")
                {
                    var efProperty = context.InnerEntityType.FindProperty(property.Name);
                    if (efProperty is not null)
                    {
                        var columnName = efProperty.GetColumnName() ?? efProperty.Name;
                        var field = new Db2FieldSchema(
                            columnName,
                            default,
                            ColumnStartIndex: -1,
                            ElementCount: 1,
                            IsVerified: false,
                            IsVirtual: false,
                            IsId: efProperty.IsPrimaryKey(),
                            IsRelation: false,
                            ReferencedTableName: null);

                        innerField = new Db2JoinedFieldAccessExpression(
                            context.InnerTableName,
                            field,
                            fieldIndex: -1,
                            property.PropertyType);
                        return true;
                    }
                }

                // Check for Outer (principal entity)
                if (parentMemberName == "Outer")
                {
                    var efProperty = context.OuterEntityType.FindProperty(property.Name);
                    if (efProperty is not null)
                    {
                        var columnName = efProperty.GetColumnName() ?? efProperty.Name;
                        var field = new Db2FieldSchema(
                            columnName,
                            default,
                            ColumnStartIndex: -1,
                            ElementCount: 1,
                            IsVerified: false,
                            IsVirtual: false,
                            IsId: efProperty.IsPrimaryKey(),
                            IsRelation: false,
                            ReferencedTableName: null);

                        outerField = new Db2FieldAccessExpression(field, fieldIndex: -1, property.PropertyType);
                        return true;
                    }
                }
            }

            // Also handle StructuralTypeShaperExpression patterns from remapped lambdas
            if (memberExpr.Expression is StructuralTypeShaperExpression shaper)
            {
                var entityType = shaper.StructuralType as IEntityType;
                if (entityType is not null)
                {
                    var isInner = entityType == context.InnerEntityType;
                    var isOuter = entityType == context.OuterEntityType;

                    if (isInner)
                    {
                        var efProperty = context.InnerEntityType.FindProperty(property.Name);
                        if (efProperty is not null)
                        {
                            var columnName = efProperty.GetColumnName() ?? efProperty.Name;
                            var field = new Db2FieldSchema(
                                columnName,
                                default,
                                ColumnStartIndex: -1,
                                ElementCount: 1,
                                IsVerified: false,
                                IsVirtual: false,
                                IsId: efProperty.IsPrimaryKey(),
                                IsRelation: false,
                                ReferencedTableName: null);

                            innerField = new Db2JoinedFieldAccessExpression(
                                context.InnerTableName,
                                field,
                                fieldIndex: -1,
                                property.PropertyType);
                            return true;
                        }
                    }

                    if (isOuter)
                    {
                        var efProperty = context.OuterEntityType.FindProperty(property.Name);
                        if (efProperty is not null)
                        {
                            var columnName = efProperty.GetColumnName() ?? efProperty.Name;
                            var field = new Db2FieldSchema(
                                columnName,
                                default,
                                ColumnStartIndex: -1,
                                ElementCount: 1,
                                IsVerified: false,
                                IsVirtual: false,
                                IsId: efProperty.IsPrimaryKey(),
                                IsRelation: false,
                                ReferencedTableName: null);

                            outerField = new Db2FieldAccessExpression(field, fieldIndex: -1, property.PropertyType);
                            return true;
                        }
                    }
                }
            }
        }

        // Handle EF.Property patterns
        if (expression is MethodCallExpression { Method: { Name: "Property", DeclaringType: { } dt } } propCall
            && dt == typeof(EF)
            && propCall.Arguments.Count == 2
            && TryEvaluateConstant(propCall.Arguments[1], out var propNameObj) && propNameObj is string propName)
        {
            // Determine if this is accessing the inner or outer entity
            var entityArg = propCall.Arguments[0];
            while (entityArg is UnaryExpression { NodeType: ExpressionType.Convert or ExpressionType.ConvertChecked } u)
                entityArg = u.Operand;

            if (entityArg is MemberExpression { Member: PropertyInfo parentProp } parentMemberExpr)
            {
                if (parentProp.Name == "Inner")
                {
                    var efProperty = context.InnerEntityType.FindProperty(propName);
                    if (efProperty is not null)
                    {
                        var columnName = efProperty.GetColumnName() ?? efProperty.Name;
                        var field = new Db2FieldSchema(
                            columnName,
                            default,
                            ColumnStartIndex: -1,
                            ElementCount: 1,
                            IsVerified: false,
                            IsVirtual: false,
                            IsId: efProperty.IsPrimaryKey(),
                            IsRelation: false,
                            ReferencedTableName: null);

                        innerField = new Db2JoinedFieldAccessExpression(
                            context.InnerTableName,
                            field,
                            fieldIndex: -1,
                            propCall.Type);
                        return true;
                    }
                }

                if (parentProp.Name == "Outer")
                {
                    var efProperty = context.OuterEntityType.FindProperty(propName);
                    if (efProperty is not null)
                    {
                        var columnName = efProperty.GetColumnName() ?? efProperty.Name;
                        var field = new Db2FieldSchema(
                            columnName,
                            default,
                            ColumnStartIndex: -1,
                            ElementCount: 1,
                            IsVerified: false,
                            IsVirtual: false,
                            IsId: efProperty.IsPrimaryKey(),
                            IsRelation: false,
                            ReferencedTableName: null);

                        outerField = new Db2FieldAccessExpression(field, fieldIndex: -1, propCall.Type);
                        return true;
                    }
                }
            }
        }

        return false;
    }

    /// <summary>
    /// Context for translating joined query predicates.
    /// </summary>
    private sealed class JoinedTranslationContext(
        ParameterExpression parameter,
        IEntityType outerEntityType,
        IEntityType innerEntityType,
        string innerTableName,
        Expression shaperExpression)
    {
        public ParameterExpression Parameter { get; } = parameter;
        public IEntityType OuterEntityType { get; } = outerEntityType;
        public IEntityType InnerEntityType { get; } = innerEntityType;
        public string InnerTableName { get; } = innerTableName;
        public Expression ShaperExpression { get; } = shaperExpression;
    }

    private Db2FilterExpression? TranslateExpression(Expression expression, IEntityType entityType, ParameterExpression parameter)
    {
        return expression switch
        {
            BinaryExpression binary => TranslateBinary(binary, entityType, parameter),
            UnaryExpression { NodeType: ExpressionType.Not } unary => TranslateNot(unary, entityType, parameter),
            MethodCallExpression methodCall => TranslateMethodCall(methodCall, entityType, parameter),
            _ => null, // Untranslatable
        };
    }

    private Db2FilterExpression? TranslateBinary(BinaryExpression binary, IEntityType entityType, ParameterExpression parameter)
    {
        // Logical operators
        if (binary.NodeType is ExpressionType.AndAlso)
        {
            var left = TranslateExpression(binary.Left, entityType, parameter);
            var right = TranslateExpression(binary.Right, entityType, parameter);
            if (left is null || right is null) return null;
            return new Db2AndFilterExpression(left, right);
        }

        if (binary.NodeType is ExpressionType.OrElse)
        {
            var left = TranslateExpression(binary.Left, entityType, parameter);
            var right = TranslateExpression(binary.Right, entityType, parameter);
            if (left is null || right is null) return null;
            return new Db2OrFilterExpression(left, right);
        }

        // Comparison operators
        var comparisonKind = binary.NodeType switch
        {
            ExpressionType.Equal => Db2ComparisonKind.Equal,
            ExpressionType.NotEqual => Db2ComparisonKind.NotEqual,
            ExpressionType.GreaterThan => Db2ComparisonKind.GreaterThan,
            ExpressionType.GreaterThanOrEqual => Db2ComparisonKind.GreaterThanOrEqual,
            ExpressionType.LessThan => Db2ComparisonKind.LessThan,
            ExpressionType.LessThanOrEqual => Db2ComparisonKind.LessThanOrEqual,
            _ => (Db2ComparisonKind?)null,
        };

        if (comparisonKind is null) return null;

        // Try collection.Count comparison: collection.Count op constant (rewriting to Any)
        if (TryTranslateCollectionCountComparison(binary.Left, binary.Right, binary.NodeType, entityType, parameter, out var countFilter)
            || TryTranslateCollectionCountComparison(binary.Right, binary.Left, FlipOperator(binary.NodeType), entityType, parameter, out countFilter))
        {
            return countFilter;
        }

        // Try string.Length comparison: field.Length op constant or constant op field.Length
        if (TryTranslateStringLengthComparison(binary.Left, binary.Right, comparisonKind.Value, entityType, parameter, out var lengthFilter)
            || TryTranslateStringLengthComparison(binary.Right, binary.Left, FlipComparison(comparisonKind.Value), entityType, parameter, out lengthFilter))
        {
            return lengthFilter;
        }

        // Try field == value or value == field
        if (TryExtractFieldAndValue(binary.Left, binary.Right, entityType, parameter, out var field, out var value)
            || TryExtractFieldAndValue(binary.Right, binary.Left, entityType, parameter, out field, out value))
        {
            // Check for null comparison
            if (value is null && comparisonKind is Db2ComparisonKind.Equal)
                return new Db2NullCheckFilterExpression(field, isNotNull: false);
            if (value is null && comparisonKind is Db2ComparisonKind.NotEqual)
                return new Db2NullCheckFilterExpression(field, isNotNull: true);

            return new Db2ComparisonFilterExpression(field, comparisonKind.Value, value);
        }

        return null;
    }

    private Db2FilterExpression? TranslateNot(UnaryExpression unary, IEntityType entityType, ParameterExpression parameter)
    {
        var inner = TranslateExpression(unary.Operand, entityType, parameter);
        return inner is null ? null : new Db2NotFilterExpression(inner);
    }

    private Db2FilterExpression? TranslateMethodCall(MethodCallExpression methodCall, IEntityType entityType, ParameterExpression parameter)
    {
        // Handle Enumerable.Contains / List.Contains (for PK multi-lookup: ids.Contains(x.Id))
        if (TryTranslateContains(methodCall, entityType, parameter, out var containsFilter))
            return containsFilter;

        // Handle string methods: Contains, StartsWith, EndsWith
        if (TryTranslateStringMatch(methodCall, entityType, parameter, out var stringFilter))
            return stringFilter;

        // Handle string.IsNullOrWhiteSpace(x)
        if (TryTranslateStringIsNullOrWhiteSpace(methodCall, entityType, parameter, out var nullOrWhiteSpaceFilter))
            return nullOrWhiteSpaceFilter;

        // Handle collection navigation .Any() calls
        if (TryTranslateCollectionAny(methodCall, entityType, parameter, out var collectionFilter))
            return collectionFilter;

        // Handle EF.Property<T>() calls
        if (TryTranslateEfProperty(methodCall, entityType, parameter, out var efPropFilter))
            return efPropFilter;

        return null;
    }

    private bool TryTranslateStringIsNullOrWhiteSpace(
        MethodCallExpression methodCall,
        IEntityType entityType,
        ParameterExpression parameter,
        out Db2FilterExpression? filter)
    {
        filter = null;

        if (methodCall.Object is not null
            || methodCall.Method.DeclaringType != typeof(string)
            || methodCall.Method.Name != nameof(string.IsNullOrWhiteSpace)
            || methodCall.Arguments.Count != 1)
        {
            return false;
        }

        if (!TryResolveFieldAccess(methodCall.Arguments[0], entityType, parameter, out var field))
            return false;

        var isNull = new Db2NullCheckFilterExpression(field, isNotNull: false);
        var isEmpty = new Db2ComparisonFilterExpression(field, Db2ComparisonKind.Equal, "");
        filter = new Db2OrFilterExpression(isNull, isEmpty);
        return true;
    }

    private bool TryTranslateContains(
        MethodCallExpression methodCall,
        IEntityType entityType,
        ParameterExpression parameter,
        out Db2FilterExpression? filter)
    {
        filter = null;

        // Static Enumerable.Contains<T>(source, item)
        if (methodCall.Method.DeclaringType == typeof(Enumerable)
            && methodCall.Method.Name == nameof(Enumerable.Contains)
            && methodCall.Arguments.Count == 2
            && TryResolveFieldAccess(methodCall.Arguments[1], entityType, parameter, out var field))
        {
            if (TryResolveCollectionSource(methodCall.Arguments[0], out var values, out var paramName))
            {
                filter = paramName is not null
                    ? new Db2ContainsFilterExpression(field, paramName)
                    : new Db2ContainsFilterExpression(field, values!);
                return true;
            }
        }

        // Instance List<T>.Contains(item) or ICollection<T>.Contains(item)
        if (methodCall.Method.Name == nameof(List<int>.Contains)
            && methodCall.Arguments.Count == 1
            && methodCall.Object is not null
            && TryResolveFieldAccess(methodCall.Arguments[0], entityType, parameter, out field))
        {
            if (TryResolveCollectionSource(methodCall.Object, out var values, out var paramName))
            {
                filter = paramName is not null
                    ? new Db2ContainsFilterExpression(field, paramName)
                    : new Db2ContainsFilterExpression(field, values!);
                return true;
            }
        }

        return false;
    }

    /// <summary>
    /// Resolves a collection expression to either concrete values or a runtime parameter name.
    /// </summary>
    private static bool TryResolveCollectionSource(
        Expression expression,
        out IReadOnlyList<object>? values,
        out string? parameterName)
    {
        values = null;
        parameterName = null;

        // Runtime parameter (extracted collection)
        if (expression is ParameterExpression paramExpr)
        {
            parameterName = paramExpr.Name!;
            return true;
        }

        // EF Core QueryParameterExpression (Extension node)
        if (expression is QueryParameterExpression queryParamExpr)
        {
            parameterName = queryParamExpr.Name;
            return true;
        }

        // Type conversions wrapping a parameter
        if (expression is UnaryExpression { NodeType: ExpressionType.Convert } unary)
        {
            return TryResolveCollectionSource(unary.Operand, out values, out parameterName);
        }

        // Concrete collection
        if (TryEvaluateConstantCollection(expression, out var concreteValues))
        {
            values = concreteValues;
            return true;
        }

        return false;
    }

    private bool TryTranslateStringMatch(
        MethodCallExpression methodCall,
        IEntityType entityType,
        ParameterExpression parameter,
        out Db2FilterExpression? filter)
    {
        filter = null;

        if (methodCall.Object is null || methodCall.Arguments.Count != 1)
            return false;

        var matchKind = methodCall.Method.Name switch
        {
            nameof(string.Contains) when methodCall.Method.DeclaringType == typeof(string) => Db2StringMatchKind.Contains,
            nameof(string.StartsWith) when methodCall.Method.DeclaringType == typeof(string) => Db2StringMatchKind.StartsWith,
            nameof(string.EndsWith) when methodCall.Method.DeclaringType == typeof(string) => Db2StringMatchKind.EndsWith,
            _ => (Db2StringMatchKind?)null,
        };

        if (matchKind is null) return false;

        if (TryResolveFieldAccess(methodCall.Object, entityType, parameter, out var field)
            && TryEvaluateConstant(methodCall.Arguments[0], out var pattern))
        {
            switch (pattern)
            {
                case string s:
                    filter = new Db2StringMatchFilterExpression(field, matchKind.Value, s);
                    return true;
                case char c:
                    filter = new Db2StringMatchFilterExpression(field, matchKind.Value, c.ToString());
                    return true;
                case Db2RuntimeParameter p:
                    filter = new Db2StringMatchFilterExpression(field, matchKind.Value, p.Name, isParameter: true);
                    return true;
            }
        }

        return false;
    }

    /// <summary>
    /// Attempts to translate collection navigation .Any() calls into an EXISTS subquery filter.
    /// Handles patterns like <c>x.Collection.Any()</c> and <c>x.Collection.Any(predicate)</c>.
    /// Also handles EF Core expanded patterns like <c>DbSet&lt;T&gt;().Where(fk == pk).Any()</c>.
    /// </summary>
    private bool TryTranslateCollectionAny(
        MethodCallExpression methodCall,
        IEntityType entityType,
        ParameterExpression parameter,
        out Db2FilterExpression? filter)
    {
        filter = null;

        // Check for Enumerable.Any() or Queryable.Any() - either Any(source) or Any(source, predicate)
        var declaringType = methodCall.Method.DeclaringType;
        if (declaringType != typeof(Enumerable) && declaringType != typeof(Queryable))
            return false;

        if (methodCall.Method.Name != nameof(Enumerable.Any))
            return false;

        if (methodCall.Arguments.Count is < 1 or > 2)
            return false;

        // Get the source collection (first argument)
        var source = methodCall.Arguments[0];

        // Unwrap any type conversions
        while (source is UnaryExpression { NodeType: ExpressionType.Convert or ExpressionType.ConvertChecked } unary)
            source = unary.Operand;

        // Try to handle EF Core expanded subquery pattern: DbSet<T>().Where(...).Any()
        if (TryTranslateExpandedSubqueryAny(source, entityType, parameter, methodCall.Arguments.Count == 2 ? methodCall.Arguments[1] : null, out filter))
            return true;

        // The source should be a member access on the entity parameter (x.Collection)
        if (source is not MemberExpression memberAccess)
            return false;

        // Verify the member access is on the entity parameter
        if (!IsEntityReference(memberAccess.Expression, parameter))
            return false;

        // Try to find the collection navigation
        var navigation = entityType.FindNavigation(memberAccess.Member.Name);
        if (navigation is null || !navigation.IsCollection)
            return false;

        // Get the foreign key information
        var foreignKey = navigation.ForeignKey;
        var targetEntityType = navigation.TargetEntityType;
        var targetTableName = targetEntityType.GetTableName() ?? targetEntityType.ClrType.Name;

        // Get the FK column on the dependent (related) entity
        // For a collection navigation, the FK is on the target (dependent) entity
        var fkProperty = foreignKey.Properties.FirstOrDefault();
        if (fkProperty is null)
            return false;

        var fkColumnName = fkProperty.GetColumnName() ?? fkProperty.Name;

        // Get the principal key (usually the Id) from the principal entity
        var principalKeyProperty = foreignKey.PrincipalKey.Properties.FirstOrDefault();
        if (principalKeyProperty is null)
            return false;

        var pkColumnName = principalKeyProperty.GetColumnName() ?? principalKeyProperty.Name;
        var pkField = new Db2FieldSchema(
            pkColumnName,
            default,
            ColumnStartIndex: -1,
            ElementCount: 1,
            IsVerified: false,
            IsVirtual: false,
            IsId: principalKeyProperty.IsPrimaryKey(),
            IsRelation: false,
            ReferencedTableName: null);

        var principalKeyField = new Db2FieldAccessExpression(pkField, fieldIndex: -1, principalKeyProperty.ClrType);

        // Handle inner predicate if present (Any(source, predicate))
        Db2FilterExpression? innerPredicate = null;
        if (methodCall.Arguments.Count == 2)
        {
            var predicateArg = methodCall.Arguments[1];
            if (predicateArg is UnaryExpression { NodeType: ExpressionType.Quote } quote)
                predicateArg = quote.Operand;

            if (predicateArg is LambdaExpression lambda)
            {
                innerPredicate = TranslateExpression(lambda.Body, targetEntityType, lambda.Parameters[0]);
                // If we can't translate the inner predicate, we still create the exists filter
                // but without the inner predicate - runtime will materialize and evaluate
            }
        }

        filter = new Db2ExistsSubqueryFilterExpression(
            targetTableName,
            fkColumnName,
            principalKeyField,
            innerPredicate);

        return true;
    }

    /// <summary>
    /// Attempts to translate an EF Core expanded subquery pattern like:
    /// <c>DbSet&lt;T&gt;().Where(m0 =&gt; EF.Property(principal, "Id") == EF.Property(m0, "FK")).Any()</c>
    /// </summary>
    private bool TryTranslateExpandedSubqueryAny(
        Expression source,
        IEntityType principalEntityType,
        ParameterExpression principalParameter,
        Expression? additionalPredicate,
        out Db2FilterExpression? filter)
    {
        filter = null;

        // Check if source is a Where() call: Enumerable.Where(source, predicate) or Queryable.Where(source, predicate)
        if (source is not MethodCallExpression whereCall)
            return false;

        var whereDeclaringType = whereCall.Method.DeclaringType;
        if ((whereDeclaringType != typeof(Enumerable) && whereDeclaringType != typeof(Queryable))
            || whereCall.Method.Name != nameof(Enumerable.Where))
            return false;

        if (whereCall.Arguments.Count != 2)
            return false;

        var whereSource = whereCall.Arguments[0];
        var wherePredicate = whereCall.Arguments[1];

        // Unwrap to get DbSet or EntityQueryable
        while (whereSource is MethodCallExpression { Method.Name: "AsQueryable" } asQueryable)
            whereSource = asQueryable.Arguments[0];

        // Check if the source is a DbSet/EntityQueryable constant or similar
        IEntityType? relatedEntityType = null;
        string? relatedTableName = null;

        // Try to get the entity type from the query root
        if (whereSource.Type.IsGenericType)
        {
            var elementType = whereSource.Type.GetGenericArguments().FirstOrDefault();
            if (elementType is not null)
            {
                // Try to find the entity type in the model
                relatedEntityType = principalEntityType.Model.FindEntityType(elementType);
                if (relatedEntityType is not null)
                {
                    relatedTableName = relatedEntityType.GetTableName() ?? relatedEntityType.ClrType.Name;
                }
            }
        }

        if (relatedEntityType is null || relatedTableName is null)
            return false;

        // Extract the predicate lambda
        if (wherePredicate is UnaryExpression { NodeType: ExpressionType.Quote } quote)
            wherePredicate = quote.Operand;

        if (wherePredicate is not LambdaExpression lambda)
            return false;

        // Try to extract FK-PK relationship from the predicate
        // Pattern: EF.Property<T>(principal, "Id") != null && object.Equals(EF.Property<T>(principal, "Id"), EF.Property<T>(dependent, "FK"))
        if (!TryExtractForeignKeyRelationship(lambda.Body, principalParameter, lambda.Parameters[0], relatedEntityType, out var fkColumnName, out var pkColumnName))
            return false;

        // Resolve the principal key property
        var principalKeyProperty = principalEntityType.FindProperty(pkColumnName);
        if (principalKeyProperty is null)
        {
            // Try case-insensitive search
            principalKeyProperty = principalEntityType.GetProperties()
                .FirstOrDefault(p => string.Equals(p.GetColumnName() ?? p.Name, pkColumnName, StringComparison.OrdinalIgnoreCase));
        }

        if (principalKeyProperty is null)
            return false;

        var pkField = new Db2FieldSchema(
            pkColumnName,
            default,
            ColumnStartIndex: -1,
            ElementCount: 1,
            IsVerified: false,
            IsVirtual: false,
            IsId: principalKeyProperty.IsPrimaryKey(),
            IsRelation: false,
            ReferencedTableName: null);

        var principalKeyField = new Db2FieldAccessExpression(pkField, fieldIndex: -1, principalKeyProperty.ClrType);

        filter = new Db2ExistsSubqueryFilterExpression(
            relatedTableName,
            fkColumnName,
            principalKeyField,
            innerPredicate: null); // TODO: Handle additional predicates

        return true;
    }

    /// <summary>
    /// Extracts foreign key relationship info from an EF Core expanded predicate.
    /// Recognizes patterns like: EF.Property(principal, "Id") != null and object.Equals(EF.Property(principal, "Id"), EF.Property(dependent, "FK"))
    /// </summary>
    private static bool TryExtractForeignKeyRelationship(
        Expression predicate,
        ParameterExpression principalParameter,
        ParameterExpression dependentParameter,
        IEntityType relatedEntityType,
        out string fkColumnName,
        out string pkColumnName)
    {
        fkColumnName = null!;
        pkColumnName = null!;

        // Handle AndAlso: nullCheck && equals
        if (predicate is BinaryExpression { NodeType: ExpressionType.AndAlso } andAlso)
        {
            // Try to find the equality comparison in either side
            if (TryExtractEqualityFromPredicate(andAlso.Right, principalParameter, dependentParameter, out fkColumnName, out pkColumnName))
                return true;
            if (TryExtractEqualityFromPredicate(andAlso.Left, principalParameter, dependentParameter, out fkColumnName, out pkColumnName))
                return true;
        }

        // Direct equality
        return TryExtractEqualityFromPredicate(predicate, principalParameter, dependentParameter, out fkColumnName, out pkColumnName);
    }

    /// <summary>
    /// Extracts FK and PK column names from an equality predicate.
    /// Handles patterns like: object.Equals(EF.Property(principal, "Id"), EF.Property(dependent, "FK"))
    /// or: EF.Property(principal, "Id") == EF.Property(dependent, "FK")
    /// </summary>
    private static bool TryExtractEqualityFromPredicate(
        Expression predicate,
        ParameterExpression principalParameter,
        ParameterExpression dependentParameter,
        out string fkColumnName,
        out string pkColumnName)
    {
        fkColumnName = null!;
        pkColumnName = null!;

        // Handle object.Equals(a, b) or Equals(a, b)
        if (predicate is MethodCallExpression { Method.Name: "Equals" } equalsCall)
        {
            Expression? left = null;
            Expression? right = null;

            if (equalsCall.Object is null && equalsCall.Arguments.Count == 2)
            {
                // Static Equals(a, b)
                left = equalsCall.Arguments[0];
                right = equalsCall.Arguments[1];
            }
            else if (equalsCall.Object is not null && equalsCall.Arguments.Count == 1)
            {
                // Instance a.Equals(b)
                left = equalsCall.Object;
                right = equalsCall.Arguments[0];
            }

            if (left is not null && right is not null)
            {
                return TryExtractFkPkFromExpressions(left, right, principalParameter, dependentParameter, out fkColumnName, out pkColumnName);
            }
        }

        // Handle direct equality: a == b
        if (predicate is BinaryExpression { NodeType: ExpressionType.Equal } eq)
        {
            return TryExtractFkPkFromExpressions(eq.Left, eq.Right, principalParameter, dependentParameter, out fkColumnName, out pkColumnName);
        }

        return false;
    }

    /// <summary>
    /// Extracts FK and PK column names from two expressions that should be EF.Property calls.
    /// </summary>
    private static bool TryExtractFkPkFromExpressions(
        Expression left,
        Expression right,
        ParameterExpression principalParameter,
        ParameterExpression dependentParameter,
        out string fkColumnName,
        out string pkColumnName)
    {
        fkColumnName = null!;
        pkColumnName = null!;

        // Unwrap casts
        while (left is UnaryExpression { NodeType: ExpressionType.Convert or ExpressionType.ConvertChecked } ul)
            left = ul.Operand;
        while (right is UnaryExpression { NodeType: ExpressionType.Convert or ExpressionType.ConvertChecked } ur)
            right = ur.Operand;

        // Try both orderings: (principal, dependent) or (dependent, principal)
        if (TryExtractPropertyCall(left, principalParameter, out var leftPropName) &&
            TryExtractPropertyCall(right, dependentParameter, out var rightPropName))
        {
            pkColumnName = leftPropName;
            fkColumnName = rightPropName;
            return true;
        }

        if (TryExtractPropertyCall(right, principalParameter, out var rightPropName2) &&
            TryExtractPropertyCall(left, dependentParameter, out var leftPropName2))
        {
            pkColumnName = rightPropName2;
            fkColumnName = leftPropName2;
            return true;
        }

        return false;
    }

    /// <summary>
    /// Extracts property name from an EF.Property call on the expected entity parameter.
    /// </summary>
    private static bool TryExtractPropertyCall(Expression expr, ParameterExpression entityParameter, out string propertyName)
    {
        propertyName = null!;

        // Handle EF.Property<T>(entity, "PropertyName")
        if (expr is not MethodCallExpression { Method: { Name: "Property", DeclaringType: { } dt } } propCall)
            return false;

        if (dt != typeof(EF))
            return false;

        if (propCall.Arguments.Count != 2)
            return false;

        // Check that the entity argument refers to our expected parameter
        var entityArg = propCall.Arguments[0];
        while (entityArg is UnaryExpression { NodeType: ExpressionType.Convert or ExpressionType.ConvertChecked } u)
            entityArg = u.Operand;

        // Compare by reference first (fast path)
        var isMatch = entityArg == entityParameter;

        // If reference comparison fails, try name/type comparison for ParameterExpressions
        // EF Core may create different parameter instances with the same identity
        if (!isMatch && entityArg is ParameterExpression pe)
        {
            isMatch = pe.Name == entityParameter.Name && pe.Type == entityParameter.Type;
        }

        // Also accept StructuralTypeShaperExpression wrapping the entity
        if (!isMatch && entityArg is StructuralTypeShaperExpression)
        {
            isMatch = true;
        }

        if (!isMatch)
            return false;

        // Get the property name
        if (!TryEvaluateConstant(propCall.Arguments[1], out var propNameObj) || propNameObj is not string name)
            return false;

        propertyName = name;
        return true;
    }

    /// <summary>
    /// Attempts to translate a string.Length comparison (e.g., x.Name.Length > 0).
    /// </summary>
    private bool TryTranslateStringLengthComparison(
        Expression candidateLength,
        Expression candidateValue,
        Db2ComparisonKind comparisonKind,
        IEntityType entityType,
        ParameterExpression parameter,
        out Db2FilterExpression? filter)
    {
        filter = null;

        // Check for field.Length pattern
        if (candidateLength is not MemberExpression { Member: PropertyInfo { Name: "Length" } lengthProp } lengthMember
            || lengthProp.DeclaringType != typeof(string))
        {
            return false;
        }

        // The Length access should be on a field (e.g., x.Name)
        if (!TryResolveFieldAccess(lengthMember.Expression!, entityType, parameter, out var field))
            return false;

        // Ensure the field is a string type
        if (field.Type != typeof(string))
            return false;

        // Extract the constant value
        if (!TryEvaluateConstant(candidateValue, out var valueObj) || valueObj is not int intValue)
            return false;

        filter = new Db2StringLengthFilterExpression(field, comparisonKind, intValue);
        return true;
    }

    /// <summary>
    /// Flips a comparison operator for when operands are swapped (e.g., 5 &lt; x.Length becomes x.Length &gt; 5).
    /// </summary>
    private static Db2ComparisonKind FlipComparison(Db2ComparisonKind kind) => kind switch
    {
        Db2ComparisonKind.GreaterThan => Db2ComparisonKind.LessThan,
        Db2ComparisonKind.GreaterThanOrEqual => Db2ComparisonKind.LessThanOrEqual,
        Db2ComparisonKind.LessThan => Db2ComparisonKind.GreaterThan,
        Db2ComparisonKind.LessThanOrEqual => Db2ComparisonKind.GreaterThanOrEqual,
        _ => kind, // Equal and NotEqual are symmetric
    };

    /// <summary>
    /// Flips an expression operator for when operands are swapped.
    /// </summary>
    private static ExpressionType FlipOperator(ExpressionType op) => op switch
    {
        ExpressionType.GreaterThan => ExpressionType.LessThan,
        ExpressionType.GreaterThanOrEqual => ExpressionType.LessThanOrEqual,
        ExpressionType.LessThan => ExpressionType.GreaterThan,
        ExpressionType.LessThanOrEqual => ExpressionType.GreaterThanOrEqual,
        _ => op, // Equal and NotEqual are symmetric
    };

    /// <summary>
    /// Attempts to translate collection.Count comparisons to EXISTS subquery filters.
    /// Rewrites patterns like <c>x.Collection.Count > 0</c>, <c>x.Collection.Count >= 1</c>,
    /// or <c>x.Collection.Count != 0</c> into <c>EXISTS (related table where FK = PK)</c>.
    /// Also handles EF Core expanded patterns like <c>DbSet&lt;T&gt;().Where(...).Count() >= 1</c>.
    /// </summary>
    private bool TryTranslateCollectionCountComparison(
        Expression candidateCount,
        Expression candidateValue,
        ExpressionType op,
        IEntityType entityType,
        ParameterExpression parameter,
        out Db2FilterExpression? filter)
    {
        filter = null;

        // Get the constant value - must be a compile-time constant, not a runtime parameter
        if (!TryEvaluateConstant(candidateValue, out var valueObj) || valueObj is Db2RuntimeParameter)
            return false;

        var constant = Convert.ToInt32(valueObj);

        // Check if this should be rewritten to Any()
        // count > 0  => Any()
        // count != 0 => Any()
        // count >= 1 => Any()
        var shouldRewriteToAny = op switch
        {
            ExpressionType.GreaterThan => constant == 0,
            ExpressionType.NotEqual => constant == 0,
            ExpressionType.GreaterThanOrEqual => constant == 1,
            _ => false,
        };

        if (!shouldRewriteToAny)
            return false;

        // Try expanded subquery Count() pattern: DbSet<T>().Where(...).Count() op constant
        if (TryTranslateExpandedSubqueryCount(candidateCount, entityType, parameter, out filter))
            return true;

        // Check for collection.Count property pattern
        if (candidateCount is not MemberExpression { Member: PropertyInfo countProp } countMember)
            return false;

        if (countProp.Name != nameof(ICollection<object>.Count))
            return false;

        // The Count access should be on a collection navigation
        var collectionAccess = countMember.Expression;
        while (collectionAccess is UnaryExpression { NodeType: ExpressionType.Convert or ExpressionType.ConvertChecked } unary)
            collectionAccess = unary.Operand;

        if (collectionAccess is not MemberExpression navMember)
            return false;

        // Verify the navigation access is on the entity parameter
        if (!IsEntityReference(navMember.Expression, parameter))
            return false;

        // Try to find the collection navigation
        var navigation = entityType.FindNavigation(navMember.Member.Name);
        if (navigation is null || !navigation.IsCollection)
            return false;

        // Get the foreign key information
        var foreignKey = navigation.ForeignKey;
        var targetEntityType = navigation.TargetEntityType;
        var targetTableName = targetEntityType.GetTableName() ?? targetEntityType.ClrType.Name;

        // Get the FK column on the dependent (related) entity
        var fkProperty = foreignKey.Properties.FirstOrDefault();
        if (fkProperty is null)
            return false;

        var fkColumnName = fkProperty.GetColumnName() ?? fkProperty.Name;

        // Get the principal key (usually the Id) from the principal entity
        var principalKeyProperty = foreignKey.PrincipalKey.Properties.FirstOrDefault();
        if (principalKeyProperty is null)
            return false;

        var pkColumnName = principalKeyProperty.GetColumnName() ?? principalKeyProperty.Name;
        var pkField = new Db2FieldSchema(
            pkColumnName,
            default,
            ColumnStartIndex: -1,
            ElementCount: 1,
            IsVerified: false,
            IsVirtual: false,
            IsId: principalKeyProperty.IsPrimaryKey(),
            IsRelation: false,
            ReferencedTableName: null);

        var principalKeyField = new Db2FieldAccessExpression(pkField, fieldIndex: -1, principalKeyProperty.ClrType);

        filter = new Db2ExistsSubqueryFilterExpression(
            targetTableName,
            fkColumnName,
            principalKeyField,
            innerPredicate: null);

        return true;
    }

    /// <summary>
    /// Attempts to translate an EF Core expanded subquery Count() pattern like:
    /// <c>DbSet&lt;T&gt;().Where(m0 =&gt; EF.Property(principal, "Id") == EF.Property(m0, "FK")).Count()</c>
    /// </summary>
    private bool TryTranslateExpandedSubqueryCount(
        Expression candidateCount,
        IEntityType principalEntityType,
        ParameterExpression principalParameter,
        out Db2FilterExpression? filter)
    {
        filter = null;

        // Check if candidateCount is a Count() method call
        if (candidateCount is not MethodCallExpression countCall)
            return false;

        var countDeclaringType = countCall.Method.DeclaringType;
        if ((countDeclaringType != typeof(Enumerable) && countDeclaringType != typeof(Queryable))
            || countCall.Method.Name != nameof(Enumerable.Count))
            return false;

        // Count() should have 1 argument (the source) - Count(source)
        if (countCall.Arguments.Count != 1)
            return false;

        var source = countCall.Arguments[0];

        // Reuse the same logic as TryTranslateExpandedSubqueryAny
        return TryTranslateExpandedSubqueryAny(source, principalEntityType, principalParameter, additionalPredicate: null, out filter);
    }

    private static bool TryTranslateEfProperty(
        MethodCallExpression methodCall,
        IEntityType entityType,
        ParameterExpression parameter,
        out Db2FilterExpression? filter)
    {
        filter = null;

        // EF.Property<T>(entity, "PropertyName") — cannot produce a filter by itself,
        // but the containing comparison should handle it.
        // For now, return false; the parent binary expression will handle it.
        return false;
    }

    private bool TryExtractFieldAndValue(
        Expression candidateField,
        Expression candidateValue,
        IEntityType entityType,
        ParameterExpression parameter,
        out Db2FieldAccessExpression field,
        out object? value)
    {
        field = null!;
        value = null;

        if (TryResolveFieldAccess(candidateField, entityType, parameter, out field!)
            && TryEvaluateConstant(candidateValue, out value))
        {
            return true;
        }

        field = null!;
        value = null;
        return false;
    }

    /// <summary>
    /// Resolves a member access expression (e.g., <c>x.Id</c>) to a <see cref="Db2FieldAccessExpression"/>.
    /// Also handles <c>EF.Property&lt;T&gt;(x, "PropertyName")</c>.
    /// After <c>RemapLambdaBody</c>, entity references appear as <see cref="StructuralTypeShaperExpression"/>
    /// rather than <see cref="ParameterExpression"/>.
    /// </summary>
    private static bool TryResolveFieldAccess(
        Expression expression,
        IEntityType entityType,
        ParameterExpression parameter,
        out Db2FieldAccessExpression fieldAccess)
    {
        fieldAccess = null!;

        // Direct property access: x.Id  or  <ShaperExpression>.Id (after RemapLambdaBody)
        if (expression is MemberExpression { Member: PropertyInfo property } memberExpr
            && IsEntityReference(memberExpr.Expression, parameter))
        {
            var efProperty = entityType.FindProperty(property.Name);
            if (efProperty is null)
                return false;

            var columnName = efProperty.GetColumnName() ?? efProperty.Name;

            var field = new Db2FieldSchema(
                columnName,
                default,
                ColumnStartIndex: -1,
                ElementCount: 1,
                IsVerified: false,
                IsVirtual: false,
                IsId: efProperty.IsPrimaryKey(),
                IsRelation: false,
                ReferencedTableName: null);

            fieldAccess = new Db2FieldAccessExpression(field, fieldIndex: -1, property.PropertyType);
            return true;
        }

        // EF.Property<T>(x, "Name")  or  EF.Property<T>(<ShaperExpression>, "Name")
        if (expression is MethodCallExpression { Method: { IsGenericMethod: true, DeclaringType: { } dt } } efPropCall
            && dt == typeof(EF)
            && efPropCall.Method.Name == nameof(EF.Property)
            && efPropCall.Arguments.Count == 2
            && IsEntityReference(efPropCall.Arguments[0], parameter)
            && TryEvaluateConstant(efPropCall.Arguments[1], out var propNameObj) && propNameObj is string propName)
        {
            var efProperty = entityType.FindProperty(propName);
            if (efProperty is null)
                return false;

            var columnName = efProperty.GetColumnName() ?? efProperty.Name;
            var field = new Db2FieldSchema(
                columnName,
                default,
                ColumnStartIndex: -1,
                ElementCount: 1,
                IsVerified: false,
                IsVirtual: false,
                IsId: efProperty.IsPrimaryKey(),
                IsRelation: false,
                ReferencedTableName: null);

            fieldAccess = new Db2FieldAccessExpression(field, fieldIndex: -1, efPropCall.Type);
            return true;
        }

        return false;
    }

    /// <summary>
    /// Checks whether the expression represents an entity reference — either the lambda parameter directly
    /// or a <see cref="StructuralTypeShaperExpression"/> (which replaces the parameter after
    /// <c>RemapLambdaBody</c> in the standard EF Core pipeline).
    /// </summary>
    private static bool IsEntityReference(Expression? expression, ParameterExpression parameter)
    {
        return expression is not null
            && (expression == parameter
                || expression is StructuralTypeShaperExpression);
    }

    /// <summary>
    /// Evaluates an expression to a constant value. Handles <see cref="ConstantExpression"/>,
    /// closures (member access on constant), EF Core runtime parameters, type conversions,
    /// and compiled lambda evaluation.
    /// </summary>
    private static bool TryEvaluateConstant(Expression expression, out object? value)
    {
        value = null;

        if (expression is ConstantExpression constant)
        {
            value = constant.Value;
            return true;
        }

        // Handle closure captures: e.g., () => someVariable  →  MemberAccess(Constant(closure), field)
        if (expression is MemberExpression { Expression: ConstantExpression capturedConst, Member: var member })
        {
            value = member switch
            {
                FieldInfo fi => fi.GetValue(capturedConst.Value),
                PropertyInfo pi => pi.GetValue(capturedConst.Value),
                _ => null,
            };
            return true;
        }

        // Handle type conversions: Convert(expr) → unwrap and evaluate inner
        if (expression is UnaryExpression { NodeType: ExpressionType.Convert } unary)
        {
            return TryEvaluateConstant(unary.Operand, out value);
        }

        // Handle EF Core runtime parameters (extracted by ParameterExtractingExpressionVisitor).
        // These are free ParameterExpression nodes whose values are resolved at runtime
        // via QueryContext.Parameters[name].
        if (expression is ParameterExpression paramExpr)
        {
            value = new Db2RuntimeParameter(paramExpr.Name!);
            return true;
        }

        // Handle EF Core QueryParameterExpression (NodeType = Extension).
        // Created by ExpressionTreeFuncletizer for closure-captured variables.
        if (expression is QueryParameterExpression queryParamExpr)
        {
            value = new Db2RuntimeParameter(queryParamExpr.Name);
            return true;
        }

        // Fallback: compile and invoke
        try
        {
            var lambda = Expression.Lambda(expression);
            var compiled = lambda.Compile();
            value = compiled.DynamicInvoke();
            return true;
        }
        catch
        {
            return false;
        }
    }

    /// <summary>
    /// Evaluates an expression to a collection of constant values (for Contains/IN operations).
    /// </summary>
    private static bool TryEvaluateConstantCollection(Expression expression, out IReadOnlyList<object> values)
    {
        values = [];

        if (!TryEvaluateConstant(expression, out var obj) || obj is null)
            return false;

        if (obj is System.Collections.IEnumerable enumerable)
        {
            var list = new List<object>();
            foreach (var item in enumerable)
                list.Add(item);
            values = list;
            return true;
        }

        return false;
    }
}
