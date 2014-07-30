// -----------------------------------------------------------------------
//  <copyright file="AbstractRaftStateBehavior.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Rhino.Raft.Commands;
using Rhino.Raft.Messages;

namespace Rhino.Raft.Behaviors
{
	public class LeaderStateBehavior : AbstractRaftStateBehavior
	{
		private readonly Dictionary<string, long> _matchIndexes = new Dictionary<string, long>();
		private readonly ConcurrentDictionary<string, long> _nextIndexes = new ConcurrentDictionary<string, long>();

		private readonly Task _heartbeatTask;

		public LeaderStateBehavior(RaftEngine engine)
			: base(engine)
		{
			Timeout = engine.MessageTimeout;

			var lastLogEntry = Engine.PersistentState.LastLogEntry() ?? new LogEntry();

			foreach (var peer in Engine.AllPeers)
			{
				_nextIndexes[peer] = lastLogEntry.Index + 1;
				_matchIndexes[peer] = 0;
			}

			AppendCommand(new NopCommand());
			_heartbeatTask = Task.Run(() => Heartbeat(), Engine.CancellationToken);
		}

		private void Heartbeat()
		{
			while (Engine.State == RaftEngineState.Leader && !Engine.CancellationToken.IsCancellationRequested)
			{
				Engine.DebugLog.Write("Leader heartbeat");
				Engine.CancellationToken.ThrowIfCancellationRequested();
				SendEntriesToAllPeers();

				Thread.Sleep(Engine.MessageTimeout/6);
			}
		}

		private void SendEntriesToAllPeers()
		{
			foreach (var peer in Engine.AllPeers)
			{
				SendEntriesToPeer(peer);
			}
		}

		private void SendEntriesToPeer(string peer)
		{
			var nextIndex = _nextIndexes[peer];
			
			var entries = Engine.PersistentState.LogEntriesAfter(nextIndex)
												.Take(Engine.MaxEntriesPerRequest)
												.ToArray();			
			_nextIndexes[peer] += entries.Length;

			var prevLogEntry = entries.Length == 0
				? Engine.PersistentState.LastLogEntry()
				: Engine.PersistentState.GetLogEntry(entries[0].Index - 1);

			prevLogEntry = prevLogEntry ?? new LogEntry();
			
			Engine.DebugLog.Write("Sending {0:#,#;;0} entries to {1}", entries.Length, peer);

			var aer = new AppendEntriesRequest
			{
				Entries = entries,
				LeaderCommit = Engine.CommitIndex,
				LeaderId = Engine.Name,
				PrevLogIndex = prevLogEntry.Index,
				PrevLogTerm = prevLogEntry.Term,
				Term = Engine.PersistentState.CurrentTerm
			};

			Engine.Transport.Send(peer, aer);
			OnEntriesAppended(entries); //equivalent to followers receiving the entries			
		}

		protected RaftEngineState State
		{
			get { return RaftEngineState.Leader; }
		}

		public override void HandleTimeout()
		{
			// we don't have to do anything here
		}

		public override void Handle(string destination, AppendEntriesResponse resp)
		{
			// there is a new leader in town, time to step down
			if (resp.CurrentTerm > Engine.PersistentState.CurrentTerm)
			{				
				Engine.UpdateCurrentTerm(resp.CurrentTerm,resp.LeaderId);
				return;
			}
			
			if (resp.Success == false)
			{
				// go back in the log, this peer isn't matching us at this location
				_nextIndexes[resp.Source] = _nextIndexes[resp.Source] - 1;
				return;
			}

			Debug.Assert(resp.Source != null);
			_matchIndexes[resp.Source] = resp.LastLogIndex;

			var maxIndexOnQuorom = GetMaxindexOnQuorom();

			if (maxIndexOnQuorom <= Engine.CommitIndex)
				return;

			Engine.ApplyCommits(Engine.CommitIndex, maxIndexOnQuorom);
		}

		private long GetMaxindexOnQuorom()
		{
			var dic = new Dictionary<long, int>();
			foreach (var matchIndex in _matchIndexes)
			{
				var index = matchIndex.Value;
				int count;
				dic.TryGetValue(index, out count);

				dic[index] = count + 1;
			}

			var boost = 0;
			foreach (var source in dic.OrderByDescending(x => x.Key))
			{
				var confirmationsForThisIndex = source.Value + boost;
				boost += source.Value;
				if (confirmationsForThisIndex >= Engine.QuorumSize)
					return source.Key;
			}

			return -1;
		}

		public void AppendCommand(Command command)
		{
			var commandEntry = Engine.CommandSerializer.Serialize(command);			
			_matchIndexes[Engine.Name] = command.AssignedIndex = Engine.PersistentState.AppendToLeaderLog(commandEntry);
		}

		public override void Dispose()
		{
			_heartbeatTask.Wait();
		}
	}
}