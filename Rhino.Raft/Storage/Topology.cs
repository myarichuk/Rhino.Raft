using System;
using System.Collections.Generic;
using System.Linq;

namespace Rhino.Raft.Storage
{
	public class Topology
	{
		public HashSet<string> AllVotingNodes { get; private set; }
		private string topologyString = string.Empty;
		private int topologyStringCount = -1;
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

		public Topology Clone()
		{
			return new Topology(AllVotingNodes);
		}

		public Topology CloneAndRemove(params string[] additionalNodes)
		{
			return new Topology(AllVotingNodes.Except(additionalNodes, StringComparer.InvariantCultureIgnoreCase));
		}

		public Topology CloneAndAdd(params string[] additionalNodes)
		{
			return new Topology(AllVotingNodes.Union(additionalNodes, StringComparer.InvariantCultureIgnoreCase));
		}

		public bool HasQuorum(HashSet<string> votes)
		{
			var sum = AllVotingNodes.Count(votes.Contains);
			return sum >= QuoromSize;
		}

		public override string ToString()
		{
			if (topologyStringCount != AllVotingNodes.Count)
			{
				topologyString = string.Join(", ", AllVotingNodes);
				topologyStringCount = AllVotingNodes.Count;
			}
			return topologyString;
		}
	}
}