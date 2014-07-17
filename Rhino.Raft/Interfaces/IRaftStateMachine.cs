using Rhino.Raft.Messages;

namespace Rhino.Raft.Interfaces
{
	public interface IRaftStateMachine
	{
		long LastApplied { get; }
		void Apply(LogEntry entry);
	}
}
