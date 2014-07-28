// -----------------------------------------------------------------------
//  <copyright file="RaftEngine.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Rhino.Raft.Behaviors;
using Rhino.Raft.Commands;
using Rhino.Raft.Interfaces;
using Rhino.Raft.Messages;
using Rhino.Raft.Storage;
using Rhino.Raft.Utils;

namespace Rhino.Raft
{
	public class RaftEngine : IDisposable
	{
		private readonly CancellationTokenSource _cancellationTokenSource;

		public DebugWriter DebugLog { get; set; }
		public ITransport Transport { get; set; }
		public IRaftStateMachine StateMachine { get; set; }
		public IEnumerable<string> AllVotingPeers { get; set; }
		public IEnumerable<string> AllPeers { get; set; }
		public string Name { get; set; }
		public PersistentState PersistentState { get; set; }
		public ICommandSerializer CommandSerializer { get; set; }
		public string CurrentLeader { get; set; }

		/// <summary>
		/// This is a thread safe operation, since this is being used by both the leader's message processing thread
		/// and the leader's heartbeat thread
		/// </summary>
		public long CommitIndex
		{
			get { return Thread.VolatileRead(ref _commitIndex); }
			set { Interlocked.Exchange(ref _commitIndex, value); }
		}

		public int QuorumSize
		{
			get { return ((AllVotingPeers.Count() + 1)/2) + 1; }
		}

		public RaftEngineState State { get; private set; }

		public int MaxEntriesPerRequest { get; set; }

		private readonly Task _eventLoopTask;

		private long _commitIndex;

		private AbstractRaftStateBehavior StateBehavior { get; set; }

		/// <summary>
		/// can be heartbeat timeout or election timeout - depends on the state behavior
		/// </summary>
		public int MessageTimeout { get; set; }
		public CancellationToken CancellationToken { get { return _cancellationTokenSource.Token; } }
		
		public event Action<RaftEngineState> StateChanged;

		public RaftEngine(RaftEngineOptions raftEngineOptions)
		{

			DebugLog = new DebugWriter(raftEngineOptions.Name, raftEngineOptions.Stopwatch);

			AllPeers = raftEngineOptions.AllPeers ?? new List<string>();
			AllVotingPeers = raftEngineOptions.AllPeers ?? new List<string>();

			CommandSerializer = new JsonCommandSerializer();

			//warm up!
			CommandSerializer.Serialize(new NopCommand());

			MessageTimeout = raftEngineOptions.MessageTimeout;
			
			_cancellationTokenSource = new CancellationTokenSource();

			MaxEntriesPerRequest = Default.MaxEntriesPerRequest;
			Name = raftEngineOptions.Name;
			PersistentState = new PersistentState(raftEngineOptions.Options, _cancellationTokenSource.Token);
			Transport = raftEngineOptions.Transport;
			StateMachine = raftEngineOptions.StateMachine;

			SetState(AllPeers.Any() ? RaftEngineState.Follower : RaftEngineState.Leader);

			_eventLoopTask = Task.Run(() => EventLoop());
		}

		protected void EventLoop()
		{
			while (_cancellationTokenSource.IsCancellationRequested == false)
			{
				try
				{
					MessageEnvelope message;
					var behavior = StateBehavior;
					var hasMessage = Transport.TryReceiveMessage(Name, behavior.Timeout, _cancellationTokenSource.Token, out message);

					if (hasMessage == false)
					{
						DebugLog.Write("State {0} timeout ({1:#,#;;0} ms).", State, behavior.Timeout);
						behavior.HandleTimeout();
						continue;
					}
					DebugLog.Write("State {0} message {1}", State, message.Message);
					behavior.HandleMessage(message);
				}
				catch (OperationCanceledException)
				{
					break;
				}
			}
		}

		internal void UpdateCurrentTerm(long term)
		{
			PersistentState.UpdateTermTo(term);
			SetState(RaftEngineState.Follower);
			CurrentLeader = null;
		}

		internal void SetState(RaftEngineState state)
		{
			if (state == State)
				return;

			State = state;
			var oldState = StateBehavior;
			using (oldState)
			{
				switch (state)
				{
					case RaftEngineState.Follower:
						StateBehavior = new FollowerStateBehavior(this);
						break;
					case RaftEngineState.Candidate:
						StateBehavior = new CandidateStateBehavior(this);
						break;
					case RaftEngineState.Leader:
						CurrentLeader = Name;
						StateBehavior = new LeaderStateBehavior(this);
						break;
				}
			}

			OnStateChanged(state);
		}

		internal bool LogIsUpToDate(long lastLogTerm, long lastLogIndex)
		{
			// Raft paper 5.4.1
			var lastLogEntry = PersistentState.LastLogEntry() ?? new LogEntry();

			if (lastLogEntry.Term < lastLogTerm)
				return true;
			return lastLogEntry.Index <= lastLogIndex;
		}

		public void AppendCommand(Command command)
		{
			var leaderStateBehavior = StateBehavior as LeaderStateBehavior;
			if(leaderStateBehavior == null)
				throw new InvalidOperationException("Command can be appended only on leader node. Leader node name is " + CurrentLeader);

			leaderStateBehavior.AppendCommand(command);
		}

		public void ApplyCommits(long from, long to)
		{
			foreach (LogEntry entry in PersistentState.LogEntriesAfter(from, to))
			{
				StateMachine.Apply(entry);
			}
			CommitIndex = to;
		}

		internal void AnnounceCandidacy()
		{
			PersistentState.IncrementTermAndVoteFor(Name);

			SetState(RaftEngineState.Candidate);

			DebugLog.Write("Calling an election in term {0}", PersistentState.CurrentTerm);

			var lastLogEntry = PersistentState.LastLogEntry() ?? new LogEntry();
			var rvr = new RequestVoteRequest
			{
				CandidateId = Name,
				LastLogIndex = lastLogEntry.Index,
				LastLogTerm = lastLogEntry.Term,
				Term = PersistentState.CurrentTerm
			};

			foreach (var votingPeer in AllVotingPeers)
			{
				Transport.Send(votingPeer, rvr);
			}
		}

		public void Dispose()
		{
			_cancellationTokenSource.Cancel();
			_eventLoopTask.Wait();
			PersistentState.Dispose();
		}

		protected virtual void OnStateChanged(RaftEngineState state)
		{
			var handler = StateChanged;
			if (handler != null) handler(state);
		}

	}

	public enum RaftEngineState
	{
		None,
		Follower,
		Leader,
		Candidate
	}
}