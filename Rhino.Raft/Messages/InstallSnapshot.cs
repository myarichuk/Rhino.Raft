namespace Rhino.Raft.Messages
{
	public class InstallSnapshot
	{
		public long Term { get; set; }

		public long LastIncludedIndex { get; set; }

		public long LastIncludedTerm { get; set; }
	}
}
