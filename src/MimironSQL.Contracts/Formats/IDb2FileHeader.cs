namespace MimironSQL.Formats;

public interface IDb2FileHeader
{
    uint LayoutHash { get; }

    int FieldsCount { get; }
}
