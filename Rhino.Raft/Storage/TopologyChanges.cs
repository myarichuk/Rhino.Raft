namespace Rhino.Raft.Storage
{
	public class TopologyChanges
	{
		public Configuration OldConfiguration { get; set; }
		
		public Configuration NewConfiguration { get; set; }

		public int OldQuorum
		{
			get { return ((OldConfiguration.AllVotingPeers.Length + 1) / 2) + 1; }
		}

		public int NewQuorum
		{
			get { return ((NewConfiguration.AllVotingPeers.Length + 1) / 2) + 1; }
		}
	}
}
