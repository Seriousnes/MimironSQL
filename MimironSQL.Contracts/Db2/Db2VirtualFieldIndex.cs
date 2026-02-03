namespace MimironSQL.Db2;

public static class Db2VirtualFieldIndex
{
    public const int Id = -1;

    // WDC5 parent relation / section parent lookup.
    public const int ParentRelation = -2;

    // Any other non-inline virtual field we don't currently support.
    public const int UnsupportedNonInline = -3;
}
