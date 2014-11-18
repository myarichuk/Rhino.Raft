namespace Rhino.Raft
{
	public enum RaftEngineState
	{
		None,
		Follower,
		Leader,
		Candidate,
		SteppingDown,
		SnapshotInstallation,

		FollowerAfterStepDown,
		CandidateByRequest,
	}
}