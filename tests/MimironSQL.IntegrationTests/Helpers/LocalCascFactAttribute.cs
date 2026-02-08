namespace MimironSQL.IntegrationTests.Helpers;

[AttributeUsage(AttributeTargets.Method, AllowMultiple = false)]
internal sealed class LocalCascFactAttribute : FactAttribute
{
    public LocalCascFactAttribute()
    {
        if (!LocalEnvLocal.TryGetWowInstallRoot(out var wowInstallRoot) || !Directory.Exists(wowInstallRoot))
        {
            Skip = $"Local CASC integration test requires '{LocalEnvLocal.GetEnvLocalPath()}' with WOW_INSTALL_ROOT=<path>.";
        }
    }
}
