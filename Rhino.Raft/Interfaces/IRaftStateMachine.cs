using System;
using System.IO;
using System.Threading.Tasks;
using Rhino.Raft.Commands;
using Rhino.Raft.Messages;

namespace Rhino.Raft.Interfaces
{
	public interface IRaftStateMachine
	{
		long LastApplied { get; }
		void Apply(LogEntry entry, Command command);

		Task CreateSnapshotAsync();

		Stream ReadSnapshot(int snapshotCutoffIndexId);

		Task WriteSnapshotAsync(Stream stream);

	}
}