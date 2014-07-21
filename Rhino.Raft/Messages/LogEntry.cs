namespace Rhino.Raft.Messages
{
	public class LogEntry
	{
		public long Index { get; set; }
		public long Term { get; set; }

        public byte[] Data { get; set; }
	}
}