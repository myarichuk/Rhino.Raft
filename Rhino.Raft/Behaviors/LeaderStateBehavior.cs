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

			foreach (var peer in Engine.AllVotingNodes)
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
			var changingTopology = Engine.ChangingTopology;
			var peers = (changingTopology == null) ? 
				Engine.AllVotingNodes :
				//if node is removed --> then we will need to send TopologyChangedCommand that will remove it from cluster and stop its messaging loop
				//if node is added --> then we will need to send TopologyChangedCommand that will effectively add it to cluster
				Engine.AllVotingNodes.Union(changingTopology.AllVotingNodes,StringComparer.OrdinalIgnoreCase);
			foreach (var peer in peers)
			{
				if(peer.Equals(Engine.Name,StringComparison.OrdinalIgnoreCase))
					continue;
				
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
				var nextIndex = _nextIndexes.GetOrAdd(peer,0); //new peer's index starts at 0

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
				Term = Engine.PersistentState.CurrentTerm,
				HasTopologyChange = entries.Any(e => e.IsTopologyChange)
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
			
			var maxIndexOnCurrentQuorom = GetMaxIndexOnQuorom();
			if (maxIndexOnCurrentQuorom == -1)
			{
				Engine.DebugLog.Write("Not enough followers committed, not applying commits yet");
				return;
			}

			if (maxIndexOnCurrentQuorom <= Engine.CommitIndex)
			{
				Engine.DebugLog.Write("maxIndexOnQuorom = {0} <= Engine.CommitIndex = {1} --> no need to apply commits",
					maxIndexOnCurrentQuorom, Engine.CommitIndex);
				return;
			}

			Engine.DebugLog.Write(
				"AppendEntriesResponse => applying commits, maxIndexOnQuorom = {0}, Engine.CommitIndex = {1}", maxIndexOnCurrentQuorom,
				Engine.CommitIndex);
			Engine.ApplyCommits(Engine.CommitIndex + 1, maxIndexOnCurrentQuorom);

			Command result;
			while (_pendingCommands.TryPeek(out result) && result.AssignedIndex <= maxIndexOnCurrentQuorom)
			{
				if (_pendingCommands.TryDequeue(out result) == false)
					break; // should never happen

				result.Completion.TrySetResult(null);
			}
		}

		/// <summary>
		/// This method works on the match indexes, assume that we have three nodes
		/// A, B and C, and they have the following index values:
		/// 
		/// { A = 4, B = 3, C = 2 }
		/// 
		/// 
		/// In this case, the quorom agrees on 3 as the committed index.
		/// 
		/// Why? Because A has 4 (which implies that it has 3) and B has 3 as well.
		/// So we have 2 nodes that have 3, so that is the quorom.
		/// </summary>
		private long GetMaxIndexOnQuorom()
		{
			var maxIndexOnQuoromForCurrentTopology = GetMaxIndexOnQuoromForTopology(Engine.CurrentTopology);
			var changingTopology = Engine.ChangingTopology;
			if (changingTopology != null)
			{
				// in this case, we have to take into account BOTH the current and changing topologies
				var maxIndexOnQuoromForChangingTopology = GetMaxIndexOnQuoromForTopology(changingTopology);

				return Math.Min(maxIndexOnQuoromForCurrentTopology, maxIndexOnQuoromForChangingTopology);
			}

			return maxIndexOnQuoromForCurrentTopology;
		}

		private long GetMaxIndexOnQuoromForTopology(Topology topology)
		{
			var dic = new Dictionary<long, int>();
			foreach (var index in _matchIndexes)
			{
				if (topology.AllVotingNodes.Contains(index.Key, StringComparer.OrdinalIgnoreCase) == false)
					continue;

				int count;
				dic.TryGetValue(index.Value, out count);

				dic[index.Value] = count + 1;
			}
			var boost = 0;
			foreach (var source in dic.OrderByDescending(x => x.Key))
			{
				var confirmationsForThisIndex = source.Value + boost;
				boost += source.Value;
				if (confirmationsForThisIndex >= topology.QuoromSize)
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