namespace Rhino.Raft.Storage
{
	public class TopologyChanges
	{
		public Topology OldTopology { get; set; }
		
		public Topology NewTopology { get; set; }

		public int OldQuorum
		{
			get { return ((OldTopology.AllVotingPeers.Length + 1) / 2) + 1; }
		}

		public int NewQuorum
		{
			get { return ((NewTopology.AllVotingPeers.Length + 1) / 2) + 1; }
		}
	}
}
