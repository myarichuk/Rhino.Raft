using System.IO;
using System.Security.Cryptography.X509Certificates;
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

		bool SupportSnapshots { get; }

		/// <summary>
		/// Create a snapshot, can be called concurrently with GetSnapshotWriter
		/// </summary>
		void CreateSnapshot(long index, long term);

		/// <summary>
		/// Can be called concurrently with CreateSnapshot
		/// Should be cheap unless WriteSnapshot is called
		/// </summary>
		ISnapshotWriter GetSnapshotWriter();

		void ApplySnapshot(long term, long index, Stream stream);
	}
}