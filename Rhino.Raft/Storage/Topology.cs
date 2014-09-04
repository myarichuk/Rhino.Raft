using System;
using System.Collections.Generic;
using System.Linq;

namespace Rhino.Raft.Storage
{
	public class Topology
	{
		public string[] AllPeers { get; private set; }

		public string[] AllVotingPeers { get; private set; }

		public Topology(IEnumerable<string> allPeers, IEnumerable<string> allVotingPeers)
		{
			if (allVotingPeers == null) throw new ArgumentNullException("allVotingPeers");
			if (allPeers == null) throw new ArgumentNullException("allPeers");

			AllVotingPeers = allVotingPeers.ToArray();
			AllPeers = allPeers.ToArray();
		}
	}
}