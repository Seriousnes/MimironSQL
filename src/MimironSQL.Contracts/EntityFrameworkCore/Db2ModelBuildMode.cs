namespace MimironSQL.EntityFrameworkCore;

/// <summary>
/// Controls how the provider builds the internal DB2 model.
/// </summary>
public enum Db2ModelBuildMode
{
    /// <summary>
    /// Build the DB2 model eagerly (resolving schemas up front).
    /// </summary>
    Eager = 0,

    /// <summary>
    /// Build the DB2 model lazily (deferring schema resolution until needed).
    /// </summary>
    Lazy = 1,
}
