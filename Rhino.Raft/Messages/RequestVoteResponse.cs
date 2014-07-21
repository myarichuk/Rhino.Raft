namespace Rhino.Raft.Messages
{
    public class RequestVoteResponse
    {
        public long Term { get; set; }
        public bool VoteGranted { get; set; }
        public string Message { get; set; }
    }
}