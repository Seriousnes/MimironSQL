using System.Linq;
using System.Reflection;

using MimironSQL.Db2;
using MimironSQL.Db2.Model;
using MimironSQL.Db2.Schema;
using MimironSQL.Db2.Wdc5;

namespace MimironSQL.Db2.Query;

internal static class Db2NavigationQueryCompiler
{
    public static bool TryCompileSemiJoinPredicate<TEntity>(
        Db2Model model,
        Db2TableSchema rootSchema,
        Func<string, (Wdc5File File, Db2TableSchema Schema)> tableResolver,
        System.Linq.Expressions.Expression<Func<TEntity, bool>> predicate,
        out Func<Wdc5Row, bool> rowPredicate)
    {
        rowPredicate = _ => true;

        if (!Db2NavigationQueryTranslator.TryTranslateStringPredicate(model, predicate, out var plan))
            return false;

        var (relatedFile, relatedSchema) = tableResolver(plan.Join.Target.TableName);

        if (!relatedSchema.TryGetField(plan.TargetStringMember.Name, out var relatedField))
            return false;

        var relatedFieldIndex = relatedField.ColumnStartIndex;

        HashSet<int> matchingIds = [];

        if (plan.MatchKind is Db2NavigationStringMatchKind.Contains or Db2NavigationStringMatchKind.StartsWith or Db2NavigationStringMatchKind.EndsWith)
        {
            var mk = plan.MatchKind switch
            {
                Db2NavigationStringMatchKind.Contains => Db2StringMatchKind.Contains,
                Db2NavigationStringMatchKind.StartsWith => Db2StringMatchKind.StartsWith,
                Db2NavigationStringMatchKind.EndsWith => Db2StringMatchKind.EndsWith,
                _ => Db2StringMatchKind.Contains,
            };

            var isDenseOptimizable = !relatedFile.Header.Flags.HasFlag(Db2Flags.Sparse) && !relatedField.IsVirtual;
            if (isDenseOptimizable)
            {
                var starts = Db2DenseStringScanner.FindStartOffsets(relatedFile.DenseStringTableBytes, plan.Needle, mk);
                var accessor = new Db2FieldAccessor(relatedField);

                foreach (var relatedRow in relatedFile.EnumerateRows())
                {
                    var ok = mk switch
                    {
                        Db2StringMatchKind.Contains => Db2DenseStringMatch.Contains(relatedRow, accessor, starts),
                        Db2StringMatchKind.StartsWith => Db2DenseStringMatch.StartsWith(relatedRow, accessor, starts),
                        Db2StringMatchKind.EndsWith => Db2DenseStringMatch.EndsWith(relatedRow, accessor, starts),
                        _ => false,
                    };

                    if (ok)
                        matchingIds.Add(relatedRow.Id);
                }
            }
            else
            {
                foreach (var relatedRow in relatedFile.EnumerateRows())
                {
                    if (!relatedRow.TryGetString(relatedFieldIndex, out var s))
                        continue;

                    var ok = plan.MatchKind switch
                    {
                        Db2NavigationStringMatchKind.Contains => s.Contains(plan.Needle, StringComparison.Ordinal),
                        Db2NavigationStringMatchKind.StartsWith => s.StartsWith(plan.Needle, StringComparison.Ordinal),
                        Db2NavigationStringMatchKind.EndsWith => s.EndsWith(plan.Needle, StringComparison.Ordinal),
                        _ => false,
                    };

                    if (ok)
                        matchingIds.Add(relatedRow.Id);
                }
            }
        }
        else
        {
            foreach (var relatedRow in relatedFile.EnumerateRows())
            {
                if (relatedRow.TryGetString(relatedFieldIndex, out var s) && s == plan.Needle)
                    matchingIds.Add(relatedRow.Id);
            }
        }

        rowPredicate = plan.Join.Kind switch
        {
            Db2ReferenceNavigationKind.ForeignKeyToPrimaryKey => CompileForeignKeySemiJoin(rootSchema, plan.Join.Navigation.SourceKeyMember, matchingIds),
            Db2ReferenceNavigationKind.SharedPrimaryKeyOneToOne => row => row.Id != 0 && matchingIds.Contains(row.Id),
            _ => _ => false,
        };

        return true;
    }

    private static Func<Wdc5Row, bool> CompileForeignKeySemiJoin(Db2TableSchema rootSchema, MemberInfo foreignKeyMember, HashSet<int> ids)
    {
        var fkField = rootSchema.Fields.FirstOrDefault(f => f.Name.Equals(foreignKeyMember.Name, StringComparison.OrdinalIgnoreCase));
        if (fkField.Name is null)
            return _ => false;

        var rootFkIndex = fkField.ColumnStartIndex;
        return row =>
        {
            var fk = Convert.ToInt32(row.GetScalar<long>(rootFkIndex));
            return fk != 0 && ids.Contains(fk);
        };
    }
}
