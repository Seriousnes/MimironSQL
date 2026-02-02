using System.Text.Json;

using Microsoft.CodeAnalysis.Text;

namespace CASC.Net.Generators;

internal static class ManifestParser
{
    public static ManifestMapping Parse(SourceText? jsonText)
    {
        if (jsonText is null)
            return ManifestMapping.Empty;

        var json = jsonText.ToString();
        if (string.IsNullOrWhiteSpace(json))
            return ManifestMapping.Empty;

        try
        {
            using var doc = JsonDocument.Parse(json);

            var map = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
            switch (doc.RootElement.ValueKind)
            {
                case JsonValueKind.Array:
                    ParseArrayRoot(doc.RootElement, map);
                    break;

                case JsonValueKind.Object:
                    ParseObjectRoot(doc.RootElement, map);
                    break;

                default:
                    return ManifestMapping.Empty;
            }

            return map.Count == 0 ? ManifestMapping.Empty : new ManifestMapping(map);
        }
        catch
        {
            return ManifestMapping.Empty;
        }
    }

    private static void ParseArrayRoot(JsonElement rootArray, Dictionary<string, int> map)
    {
        foreach (var element in rootArray.EnumerateArray())
        {
            if (element.ValueKind != JsonValueKind.Object)
                continue;

            // Primary expected schema: { tableName, db2FileDataID }
            if (TryReadArrayElement(element, out var tableName, out var fileDataId))
                map[tableName] = fileDataId;
        }
    }

    private static bool TryReadArrayElement(JsonElement element, out string tableName, out int fileDataId)
    {
        tableName = string.Empty;
        fileDataId = 0;

        if (!TryGetStringProperty(element, "tableName", out tableName))
            return false;

        if (!TryGetIntProperty(element, "db2FileDataID", out fileDataId)
            && !TryGetIntProperty(element, "db2FileDataId", out fileDataId)
            && !TryGetIntProperty(element, "db2FileDataId", out fileDataId)
            && !TryGetIntProperty(element, "db2FileDataID", out fileDataId))
        {
            // Also tolerate alternate names
            if (!TryGetIntProperty(element, "fileDataId", out fileDataId)
                && !TryGetIntProperty(element, "fileDataID", out fileDataId)
                && !TryGetIntProperty(element, "fdid", out fileDataId))
            {
                return false;
            }
        }

        return true;
    }

    private static void ParseObjectRoot(JsonElement rootObject, Dictionary<string, int> map)
    {
        // Tolerate either:
        // 1) a dictionary-style object: { "SpellName": 123, ... }
        // 2) a wrapper object containing a "tables" array with the standard schema
        if (rootObject.TryGetProperty("tables", out var tablesProp) && tablesProp.ValueKind == JsonValueKind.Array)
        {
            ParseArrayRoot(tablesProp, map);
            return;
        }

        foreach (var prop in rootObject.EnumerateObject())
        {
            var key = prop.Name.Trim();
            if (string.IsNullOrWhiteSpace(key))
                continue;

            var value = prop.Value;
            if (value.ValueKind == JsonValueKind.Number && value.TryGetInt32(out var fdid))
            {
                map[key] = fdid;
                continue;
            }

            if (value.ValueKind == JsonValueKind.Object)
            {
                if (TryReadArrayElement(value, out var tableName, out var fileDataId))
                {
                    map[tableName] = fileDataId;
                    continue;
                }

                if (TryGetIntProperty(value, "db2FileDataID", out fdid)
                    || TryGetIntProperty(value, "fileDataId", out fdid)
                    || TryGetIntProperty(value, "fdid", out fdid))
                {
                    map[key] = fdid;
                }
            }
        }
    }

    private static bool TryGetStringProperty(JsonElement element, string name, out string value)
    {
        value = string.Empty;
        if (!element.TryGetProperty(name, out var prop) || prop.ValueKind != JsonValueKind.String)
            return false;

        var s = prop.GetString();
        if (s is null)
            return false;

        if (string.IsNullOrWhiteSpace(s))
            return false;

        value = s.Trim();
        return true;
    }

    private static bool TryGetIntProperty(JsonElement element, string name, out int value)
    {
        value = 0;
        if (!element.TryGetProperty(name, out var prop))
            return false;

        return prop.ValueKind == JsonValueKind.Number && prop.TryGetInt32(out value);
    }
}
