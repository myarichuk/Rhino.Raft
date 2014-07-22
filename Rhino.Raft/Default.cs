using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Rhino.Raft
{
	public static class Default
	{
		public static readonly TimeSpan HeartbeatTimeout = TimeSpan.FromSeconds(10);
		public const int MaxEntriesPerRequest = 256;
	}
}
