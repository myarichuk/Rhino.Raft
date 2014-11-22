namespace Rhino.Raft.Messages
{
	public abstract class BaseMessage
	{
		public string From { get; set; }
	}

	public class DisconnectedFromCluster : BaseMessage
	{
		public long Term { get; set; }
	}

	public class NothingToDo { }
}