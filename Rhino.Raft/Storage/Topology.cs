using System;
using System.Collections.Generic;
using System.Linq;

namespace Rhino.Raft.Storage
{
	public class Topology
	{
		public HashSet<string> AllVotingNodes { get; private set; }

		public int QuoromSize
		{
			get { return (AllVotingNodes.Count / 2) + 1; }
		}

		public Topology(IEnumerable<string> allVotingPeers)
		{
			AllVotingNodes = (allVotingPeers == null) ? new HashSet<string>(StringComparer.InvariantCultureIgnoreCase) : new HashSet<string>(allVotingPeers, StringComparer.InvariantCultureIgnoreCase);
		}
	}
}