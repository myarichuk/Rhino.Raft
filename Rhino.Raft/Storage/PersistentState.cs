using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using Consensus.Raft;
using Rhino.Raft.Messages;
using Voron;
using Voron.Util.Conversion;

namespace Rhino.Raft.Storage
{
	/// <summary>
	/// Uses Voron to store the persistent state / log of the raft state machine.
	/// Structure:
	/// 
	/// * $metadata tree - db id, version, current term, voted for
	/// * logs - the actual entry logs
	/// * entry-terms - the term for each entry id
	/// * peers - the data about the peers in the cluster
	/// </summary>
	public class PersistentState : IDisposable
	{
		private const string CurrentVersion = "1.0";
		private const string LogsTreeName = "logs";
		private const string EntryTermsTreeName = "entry-terms";
		public Guid DbId { get; private set; }
		public string VotedFor { get; private set; }
		public long CurrentTerm { get; private set; }

		private readonly StorageEnvironment _env;

		private readonly CancellationToken _cancellationToken;

		public PersistentState(StorageEnvironmentOptions options,  CancellationToken cancellationToken)
		{
			_cancellationToken = cancellationToken;
			_env = new StorageEnvironment(options);
			InitializeDatabase();
		}

		private void InitializeDatabase()
		{
			using (var tx = _env.NewTransaction(TransactionFlags.ReadWrite))
			{
				_env.CreateTree(tx, LogsTreeName);
				_env.CreateTree(tx, EntryTermsTreeName);

				var metadata = _env.CreateTree(tx, "$metadata");
				var versionReadResult = metadata.Read("version");
				if (versionReadResult == null) // new db
				{
					metadata.Add("version", Encoding.UTF8.GetBytes(CurrentVersion));
					DbId = Guid.NewGuid();
					metadata.Add("db-id", DbId.ToByteArray());
					metadata.Add("current-term", BitConverter.GetBytes(0L));
					metadata.Add("voted-for", Encoding.UTF8.GetBytes(string.Empty));
				}
				else
				{
					var dbVersion = versionReadResult.Reader.ToStringValue();
					if (dbVersion != CurrentVersion)
						throw new InvalidOperationException("Cannot open db because its version is " + dbVersion +
															" but the library expects version " + CurrentVersion);

					int used;
					var bytes = metadata.Read("db-id").Reader.ReadBytes(16, out used);
					DbId = new Guid(bytes);

					CurrentTerm = metadata.Read("current-term").Reader.ReadLittleEndianInt64();
					var votedFor = metadata.Read("voted-for");
					VotedFor = votedFor.Reader.Length == 0 ? null : votedFor.Reader.ToStringValue();
				}

				tx.Commit();
			}
		}

		public long? TermFor(long logIndex)
		{
			using (var tx = _env.NewTransaction(TransactionFlags.Read))
			{
				var terms = tx.ReadTree(EntryTermsTreeName);
				var key = new Slice(EndianBitConverter.Big.GetBytes(logIndex));
				var result = terms.Read(key);
				if (result == null)
					return null;
				var term = result.Reader.ReadLittleEndianInt64();

				tx.Commit();
				return term;
			}
		}

		public LogEntry LastLogEntry()
		{
			using (var tx = _env.NewTransaction(TransactionFlags.Read))
			{
				var terms = tx.ReadTree(EntryTermsTreeName);
				var logs = tx.ReadTree(LogsTreeName);

				var lastKey = logs.LastKeyOrDefault();
				if (lastKey == null)
					return null;

				var index = lastKey.CreateReader().ReadBigEndianInt64();

				var result = terms.Read(lastKey);

				var term = result.Reader.ReadLittleEndianInt64();

				tx.Commit(); 
				
				return new LogEntry
				{
					Term = term,
					Index = index
				};
			}
		}

		public LogEntry GetLogEntry(long index)
		{
			using (var tx = _env.NewTransaction(TransactionFlags.Read))
			{
				var terms = tx.ReadTree(EntryTermsTreeName);

				var result = terms.Read(new Slice(EndianBitConverter.Big.GetBytes(index)));
				if (result == null)
					return null;

				var term = result.Reader.ReadLittleEndianInt64();

				tx.Commit();

				return new LogEntry
				{
					Term = term,
					Index = index
				};
			}
		}

		public void RecordVoteFor(string candidateId)
		{
			if (string.IsNullOrEmpty(candidateId)) 
				throw new ArgumentNullException("candidateId");

			using (var tx = _env.NewTransaction(TransactionFlags.ReadWrite))
			{
				VotedFor = candidateId;
				var metadata = tx.ReadTree("$metadata");
				metadata.Add("voted-for", Encoding.UTF8.GetBytes(candidateId));

				tx.Commit();
			}
		}

		public void IncrementTermAndVoteFor(string name)
		{
			using (var tx = _env.NewTransaction(TransactionFlags.ReadWrite))
			{
				var metadata = tx.ReadTree("$metadata");
				CurrentTerm++;
				metadata.Add("current-term", BitConverter.GetBytes(CurrentTerm));
				metadata.Add("voted-for", Encoding.UTF8.GetBytes(name)); 
				tx.Commit();
			}
		}

		public void UpdateTermTo(long term)
		{
			if (term < CurrentTerm)
				throw new ArgumentException("Cannot update the term to a term that isn't greater than the current term");

			using (var tx = _env.NewTransaction(TransactionFlags.ReadWrite))
			{
				var metadata = tx.ReadTree("$metadata");

				metadata.Add("current-term", BitConverter.GetBytes(term));
				metadata.Add("voted-for", new byte[0]); // clearing who we voted for

				tx.Commit();
			}
		}

		public IEnumerable<LogEntry> LogEntriesAfter(long index, long stopAfter = long.MaxValue)
		{
			using (var tx = _env.NewTransaction(TransactionFlags.Read))
			{
				var logs = tx.ReadTree(LogsTreeName);
				var terms = tx.ReadTree(EntryTermsTreeName);
				using (var it = logs.Iterate())
				{
					var key = new Slice(EndianBitConverter.Big.GetBytes(index));
					if (it.Seek(key) == false)
						yield break;

					while (_cancellationToken.IsCancellationRequested == false)
					{
						var entryIndex = it.CurrentKey.CreateReader().ReadBigEndianInt64();
						if(entryIndex > stopAfter)
							yield break;

						var term = terms.Read(it.CurrentKey).Reader.ReadLittleEndianInt64();

						var entryReader = it.CreateReaderForCurrent();
						var buffer = new byte[entryReader.Length];
						entryReader.Read(buffer, 0, buffer.Length);
						
						yield return new LogEntry
						{
							Term = term,
							Data = buffer,
							Index = entryIndex
						};

						if (it.MoveNext() == false)
							yield break;
					}
				}
				
				tx.Commit();
			}
		}

		public void Dispose()
		{
			if (_env != null)
				_env.Dispose();
		}

		public void AppendToLog(IEnumerable<LogEntry> entries, long removeAllAfter)
		{
			using (var tx = _env.NewTransaction(TransactionFlags.ReadWrite))
			{
				var logs = tx.ReadTree(LogsTreeName);
				var terms = tx.ReadTree(EntryTermsTreeName);

				using (var it = logs.Iterate())
				{
					if (it.Seek(new Slice(EndianBitConverter.Big.GetBytes(removeAllAfter))) && 
						it.MoveNext())
					{
						while (it.DeleteCurrentAndMoveNext())
						{
							// delete everything from here on forward	
						}
					}
				}


				foreach (var entry in entries)
				{
					var key = new Slice(EndianBitConverter.Big.GetBytes(entry.Index));
					logs.Add(key, entry.Data);
					terms.Add(key, new Slice(BitConverter.GetBytes(entry.Term)));
				}

				tx.Commit();
			}
		}

	}
}
