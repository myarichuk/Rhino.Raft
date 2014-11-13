namespace Rhino.Raft.Messages
{
	public class CanInstallSnapshotResponse : BaseMessage
	{
		public bool IsCurrentlyInstalling { get; set; }

		public bool Success { get; set; }
		public string Message { get; set; }
	}
}