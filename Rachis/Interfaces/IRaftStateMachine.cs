using System.IO;
using Rachis.Commands;
using Rachis.Messages;

namespace Rachis.Interfaces
{
	public interface IRaftStateMachine
	{
		long LastAppliedIndex { get; }

		int EntryCount { get; }

		void Apply(LogEntry entry, Command cmd);

		bool SupportSnapshots { get; }

		/// <summary>
		/// Create a snapshot, can be called concurrently with GetSnapshotWriter, can also be called concurrently
		/// with calls to Apply.
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