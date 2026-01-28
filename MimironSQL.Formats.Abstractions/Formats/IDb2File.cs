namespace MimironSQL.Formats;

public interface IDb2File
{
    Db2FileLayout Layout { get; }
    
    IEnumerable<IDb2Row> EnumerateRows();
    
    bool TryGetRowById<TId>(TId id, out IDb2Row row) where TId : System.Numerics.IBinaryInteger<TId>;
}
