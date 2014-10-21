// -----------------------------------------------------------------------
//  <copyright file="AbstractRaftStateBehavior.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Rhino.Raft.Commands;
using Rhino.Raft.Messages;
using Rhino.Raft.Storage;

namespace Rhino.Raft.Behaviors
{
	public class LeaderStateBehavior : AbstractRaftStateBehavior
	{
		private readonly ConcurrentDictionary<string, long> _matchIndexes = new ConcurrentDictionary<string, long>();
		private readonly ConcurrentDictionary<string, long> _nextIndexes = new ConcurrentDictionary<string, long>();

		private readonly ConcurrentQueue<Command> _pendingCommands = new ConcurrentQueue<Command>();
		private readonly Task _heartbeatTask;

		private readonly CancellationTokenSource _disposedCancellationTokenSource = new CancellationTokenSource();
		private readonly CancellationTokenSource _stopHeartbeatCancellationTokenSource;

		public LeaderStateBehavior(RaftEngine engine)
			: base(engine)
		{
			Timeout = engine.MessageTimeout;

			var lastLogEntry = Engine.PersistentState.LastLogEntry() ?? new LogEntry();

			foreach (var peer in Engine.AllVotingPeers)
			{
				_nextIndexes[peer] = lastLogEntry.Index + 1;
				_matchIndexes[peer] = 0;
			}

			AppendCommand(new NopCommand());
			_stopHeartbeatCancellationTokenSource = CancellationTokenSource.CreateLinkedTokenSource(Engine.CancellationToken, _disposedCancellationTokenSource.Token);

			_heartbeatTask = Task.Run(() => Heartbeat(), _stopHeartbeatCancellationTokenSource.Token);

		}

		private void Heartbeat()
		{
			while (_stopHeartbeatCancellationTokenSource.IsCancellationRequested == false)
			{
				Engine.DebugLog.Write("Starting sending Leader heartbeats");
				SendEntriesToAllPeers();

				Thread.Sleep(Engine.MessageTimeout / 6);
			}
		}

		private void SendEntriesToAllPeers()
		{
			foreach (var peer in Engine.AllVotingPeers)
			{
				_stopHeartbeatCancellationTokenSource.Token.ThrowIfCancellationRequested();
				SendEntriesToPeer(peer);
			}
		}

		private void SendEntriesToPeer(string peer)
		{
			LogEntry prevLogEntry;
			LogEntry[] entries;
			try
			{
				var nextIndex = _nextIndexes[peer];

				entries = Engine.PersistentState.LogEntriesAfter(nextIndex)
					.Take(Engine.MaxEntriesPerRequest)
					.ToArray();

				prevLogEntry = entries.Length == 0
					? Engine.PersistentState.LastLogEntry()
					: Engine.PersistentState.GetLogEntry(entries[0].Index - 1);
			}
			catch (Exception e)
			{
				Engine.DebugLog.Write("Error while fetching entries from persistent state. Reason : {0}",e);
				throw;
			}

			Engine.DebugLog.Write("SendEntriesToPeer({0}) --> prevLogEntry.Index == {1}",peer,(prevLogEntry == null) ? "null" : prevLogEntry.Index.ToString(CultureInfo.InvariantCulture));
			prevLogEntry = prevLogEntry ?? new LogEntry();

			Engine.DebugLog.Write("Sending {0:#,#;;0} entries to {1}. Entry indexes: {2}", entries.Length, peer, String.Join(",", entries.Select(x => x.Index)));

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

		public override RaftEngineState State
		{
			get { return RaftEngineState.Leader; }
		}

		public override void HandleTimeout()
		{
			// we don't have to do anything here
		}

		public override void Handle(string destination, AppendEntriesResponse resp)
		{			
			Engine.DebugLog.Write("Handling AppendEntriesResponse from {0}", resp.Source);

			// there is a new leader in town, time to step down
			if (resp.CurrentTerm > Engine.PersistentState.CurrentTerm)
			{
				Engine.UpdateCurrentTerm(resp.CurrentTerm, resp.LeaderId);
				return;
			}
			
			if (resp.Success == false)
			{
				// go back in the log, this peer isn't matching us at this location				
				_nextIndexes[resp.Source] -= 1;
				Engine.DebugLog.Write("received Success = false in AppendEntriesResponse. Now _nextIndexes[{1}] = {0}.",
					_nextIndexes[resp.Source], resp.Source);
				return;
			}

			Debug.Assert(resp.Source != null);
			_matchIndexes[resp.Source] = resp.LastLogIndex;
			_nextIndexes[resp.Source] = resp.LastLogIndex + 1;
			Engine.DebugLog.Write("follower (name={0}) has LastLogIndex = {1}", resp.Source, resp.LastLogIndex);
			
			// allow to run on both quoroms!			

			var maxIndexOnQuorom = GetMaxIndexOnQuorom();
			if (maxIndexOnQuorom == -1)
			{
				Engine.DebugLog.Write("Not enough followers committed, not applying commits yet");
				return;
			}

			if (maxIndexOnQuorom <= Engine.CommitIndex)
			{
				Engine.DebugLog.Write("maxIndexOnQuorom = {0} <= Engine.CommitIndex = {1} --> no need to apply commits",
					maxIndexOnQuorom, Engine.CommitIndex);
				return;
			}

			Engine.DebugLog.Write(
				"AppendEntriesResponse => applying commits, maxIndexOnQuorom = {0}, Engine.CommitIndex = {1}", maxIndexOnQuorom,
				Engine.CommitIndex);
			Engine.ApplyCommits(Engine.CommitIndex + 1, maxIndexOnQuorom);

			Command result;
			while (_pendingCommands.TryPeek(out result) && result.AssignedIndex <= maxIndexOnQuorom)
			{
				if (_pendingCommands.TryDequeue(out result) == false)
					break; // should never happen

				result.Completion.TrySetResult(null);
			}
		}

		private long GetMaxIndexOnQuorom()
		{
			var currentConfigDictionary = CountIndexPerPeer(_matchIndexes);
			return MaxIndexOnQuorom(currentConfigDictionary, Engine.QuorumSize);
		}

		private Dictionary<long, int> CountIndexPerPeer(IEnumerable<KeyValuePair<string, long>> matchIndexes)
		{
			var dic = new Dictionary<long, int>();
			foreach (var index in matchIndexes.Select(matchIndex => matchIndex.Value))
			{
				int count;
				dic.TryGetValue(index, out count);

				dic[index] = count + 1;
			}
			return dic;
		}

		private long MaxIndexOnQuorom(Dictionary<long, int> dic, int quorumSize)
		{
			var boost = 0;
			foreach (var source in dic.OrderByDescending(x => x.Key))
			{
				var confirmationsForThisIndex = source.Value + boost;
				boost += source.Value;
				if (confirmationsForThisIndex >= quorumSize)
					return source.Key;
			}

			return -1;
		}

		public void AppendCommand(Command command)
		{
			_matchIndexes[Engine.Name] = Engine.PersistentState.AppendToLeaderLog(command);
			if (command.Completion != null)
				_pendingCommands.Enqueue(command);
		}

		public override void Dispose()
		{
			_disposedCancellationTokenSource.Cancel();
			try
			{
				_heartbeatTask.Wait(Timeout*2);
			}
			catch (OperationCanceledException)
			{
				//expected
			}
			catch (AggregateException e)
			{
				if (e.InnerException is OperationCanceledException == false)
					throw;
			}
		}
	}
}