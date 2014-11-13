namespace Rhino.Raft.Messages
{
	public class InstallSnapshotRequest : BaseMessage
	{
		public long Term { get; set; }

		public long LastIncludedIndex { get; set; }

		public long LastIncludedTerm { get; set; }
		public string LeaderId { get; set; }
	}

	public class InstallSnapshotResponse : BaseMessage
	{
		public string Message { get; set; }
		public bool Success { get; set; }
		public long CurrentTerm { get; set; }
		public long LastLogIndex { get; set; }

	}
}
