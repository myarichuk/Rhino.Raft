namespace Rhino.Raft.Messages
{
	public class AppendEntriesResponse
	{
		public long CurrentTerm { get; set; }
		public bool Success { get; set; }
		public string Message { get; set; }
	}
}