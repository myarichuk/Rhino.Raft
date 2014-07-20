namespace Rhino.Raft.Messages
{
	public class MessageEnvelope
	{
		public string Source { get; set; }
		public object Message { get; set; }
	}
}
