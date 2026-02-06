using System;
using System.Collections.Generic;
using System.Text;

using MimironSQL.Db2.Model;

namespace MimironSQL;

public partial class WoWDb2Context
{
    public override void OnModelCreating(Db2ModelBuilder modelBuilder)
    {
        base.OnModelCreating(modelBuilder);
    }
}
