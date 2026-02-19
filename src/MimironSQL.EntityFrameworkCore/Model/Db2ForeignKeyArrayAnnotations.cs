using Microsoft.EntityFrameworkCore.Metadata;

namespace MimironSQL.EntityFrameworkCore.Model;

internal static class Db2ForeignKeyArrayAnnotations
{
    internal const string ForeignKeyArrayPropertyNameAnnotation = "MimironDb2:ForeignKeyArrayPropertyName";

    public static void SetForeignKeyArrayPropertyName(IMutableNavigation navigation, string propertyName)
    {
        ArgumentNullException.ThrowIfNull(navigation);
        ArgumentException.ThrowIfNullOrWhiteSpace(propertyName);

        navigation.SetAnnotation(ForeignKeyArrayPropertyNameAnnotation, propertyName);
    }

    public static bool TryGetForeignKeyArrayPropertyName(IReadOnlyNavigation navigation, out string propertyName)
    {
        ArgumentNullException.ThrowIfNull(navigation);

        if (navigation.FindAnnotation(ForeignKeyArrayPropertyNameAnnotation) is { Value: string s } && !string.IsNullOrWhiteSpace(s))
        {
            propertyName = s;
            return true;
        }

        propertyName = string.Empty;
        return false;
    }
}
