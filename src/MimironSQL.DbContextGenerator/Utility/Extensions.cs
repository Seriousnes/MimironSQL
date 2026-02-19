using System;
using System.Collections.Generic;
using System.Text;

namespace MimironSQL.DbContextGenerator.Utility;

internal static class Extensions
{
    public static string EscapeString(this string value)
        => value.Replace("\\", "\\\\").Replace("\"", "\\\"");
}
