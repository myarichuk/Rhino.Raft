namespace Rhino.Raft.Messages
{
	public class InstallSnapshot
	{
		public long Term { get; set; }

		public byte[] Data { get; set; }

		public long LastIncludedIndex { get; set; }

		public long LastIncludedTerm { get; set; }

		public byte Offset { get; set; }
	}
}
