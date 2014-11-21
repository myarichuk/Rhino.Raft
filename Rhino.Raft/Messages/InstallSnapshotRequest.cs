using Rhino.Raft.Storage;

namespace Rhino.Raft.Messages
{
	public class InstallSnapshotRequest : BaseMessage
	{
		public long Term { get; set; }

		public long LastIncludedIndex { get; set; }

		public long LastIncludedTerm { get; set; }

		public Topology Topology { get; set; }
	}
}
