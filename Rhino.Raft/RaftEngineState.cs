namespace Rhino.Raft
{
	public enum RaftEngineState
	{
		None,
		Follower,
		Leader,
		Candidate,
		CandidateByRequest,
		SteppingDown,
		SnapshotInstallation,
	}
}