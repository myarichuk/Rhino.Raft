using System.IO;
using System.Threading.Tasks;
using Rhino.Raft.Commands;
using Rhino.Raft.Messages;
using Rhino.Raft.Storage;

namespace Rhino.Raft.Interfaces
{
	public interface IRaftStateMachine
	{
		long LastAppliedIndex { get; }

		int EntryCount { get; }

		void Apply(LogEntry entry, Command cmd);

		void CreateSnapshot();

		void WriteSnapshot(Stream stream);

		void ApplySnapshot(Stream stream);
	}
}