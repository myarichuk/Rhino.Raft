namespace Rhino.Raft.Messages
{
	public class CanInstallSnapshotRequest : BaseMessage
	{
		public long Index { get; set; }

		public long Term { get; set; }

		public string LeaderId { get; set; }
	}
}