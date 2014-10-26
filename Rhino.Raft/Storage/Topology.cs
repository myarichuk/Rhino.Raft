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
			AllVotingNodes = (allVotingPeers == null) ? 
				new HashSet<string>(StringComparer.InvariantCultureIgnoreCase) : 
				new HashSet<string>(allVotingPeers, StringComparer.InvariantCultureIgnoreCase);
		}
		public Topology CloneAndRemove(params string[] additionalNodes)
		{
			return new Topology(AllVotingNodes.Except(additionalNodes, StringComparer.InvariantCultureIgnoreCase));
		}

		public Topology CloneAndAdd(params string[] additionalNodes)
		{
			return new Topology(AllVotingNodes.Union(additionalNodes, StringComparer.InvariantCultureIgnoreCase));
		}
	}
}