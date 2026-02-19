using System;
using System.Collections.Generic;
using System.Text;

namespace MimironSQL.IntegrationTests.Helpers;

public sealed class ShouldlyScope : IDisposable
{
    private readonly List<Exception> _failures = new();

    public void Run(Action assertion)
    {
        try
        {
            assertion();
        }
        catch (Exception ex) // typically ShouldAssertException
        {
            _failures.Add(ex);
        }
    }

    public void Dispose()
    {
        if (_failures.Count == 0)
            return;

        var message = string.Join(
            Environment.NewLine + Environment.NewLine,
            _failures.Select(f => f.Message));

        throw new AggregateException(
            "Multiple assertion failures:" + Environment.NewLine + message,
            _failures);
    }
}
