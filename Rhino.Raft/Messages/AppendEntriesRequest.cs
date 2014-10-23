namespace Rhino.Raft.Messages
{
	public class AppendEntriesRequest
	{
		public long Term { get; set; }
		public string LeaderId { get; set; }
		public long PrevLogIndex { get; set; }
		public long PrevLogTerm { get; set; }
		public LogEntry[] Entries { get; set; }
		public bool HasTopologyChange { get; set; }
		public long LeaderCommit { get; set; }

	}
}