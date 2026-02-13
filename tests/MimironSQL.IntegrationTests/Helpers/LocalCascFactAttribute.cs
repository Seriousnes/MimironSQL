namespace MimironSQL.IntegrationTests.Helpers;

[AttributeUsage(AttributeTargets.Method, AllowMultiple = false)]
internal sealed class LocalCascFactAttribute : FactAttribute
{
    public LocalCascFactAttribute()
    {
        if (!LocalEnvLocal.TryGetWowInstallRoot(out var wowInstallRoot) || !Directory.Exists(wowInstallRoot))
        {
            Skip = $"Local CASC integration test requires '{LocalEnvLocal.GetEnvLocalPath()}' with WOW_INSTALL_ROOT=<path>.";
            return;
        }

        var shmemPath = Path.Combine(wowInstallRoot, "Data", "data", "shmem");
        if (!File.Exists(shmemPath))
        {
            return;
        }

        try
        {
            using var _ = new FileStream(shmemPath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite | FileShare.Delete);
        }
        catch (IOException)
        {
            Skip = $"Local CASC integration test requires exclusive access to '{shmemPath}'. Close WoW (or any process holding the file) and retry.";
        }
    }
}
