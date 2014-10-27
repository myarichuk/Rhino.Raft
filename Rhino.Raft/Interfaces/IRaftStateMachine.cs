using System;
using System.IO;
using System.Threading.Tasks;
using Rhino.Raft.Commands;
using Rhino.Raft.Messages;

namespace Rhino.Raft.Interfaces
{
	public interface IRaftStateMachine
	{
		long LastAppliedIndex { get; }
		void Apply(LogEntry entry, Command cmd);

		Task CreateSnapshotAsync();

		Stream ReadSnapshot(int snapshotIndexId);

		Task WriteSnapshotAsync(Stream stream);

	}
}