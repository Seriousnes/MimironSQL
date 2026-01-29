namespace MimironSQL.Formats;

public interface IDb2Row
{
    T Get<T>(int fieldIndex);
}
