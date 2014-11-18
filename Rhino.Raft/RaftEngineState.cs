namespace Rhino.Raft
{
	public enum RaftEngineState
	{
		None,
		Follower,
		Leader,
		Candidate,
		SnapshotInstallation
	}
}