namespace Rhino.Raft.Messages
{
	public class TimeoutNowRequest : BaseMessage
	{
		public long Term { get; set; }
	}
}