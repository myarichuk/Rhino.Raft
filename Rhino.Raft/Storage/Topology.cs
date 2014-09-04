using System;
using System.Collections.Generic;
using System.Linq;

namespace Rhino.Raft.Storage
{
	public class Topology
	{
		public string[] AllVotingPeers { get; private set; }

		public Topology(IEnumerable<string> allVotingPeers)
		{
			if (allVotingPeers == null) throw new ArgumentNullException("allVotingPeers");

			AllVotingPeers = allVotingPeers.ToArray();
		}
	}
}