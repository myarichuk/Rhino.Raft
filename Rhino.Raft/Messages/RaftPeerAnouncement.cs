using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Rhino.Raft.Messages
{
	public class RaftPeerAnouncement
	{
		public string Name { get; set; }
		public bool Voting { get; set; }
	}
}
