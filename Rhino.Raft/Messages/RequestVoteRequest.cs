namespace Rhino.Raft.Messages
{
	public class RequestVoteRequest : BaseMessage
	{
		public long Term { get; set; }
		public string CandidateId { get; set; }
		public long LastLogIndex { get; set; }
		public long LastLogTerm { get; set; }
	}
}