// -----------------------------------------------------------------------
//  <copyright file="RaftEngine.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Rhino.Raft.Behaviors;
using Rhino.Raft.Commands;
using Rhino.Raft.Interfaces;
using Rhino.Raft.Messages;
using Rhino.Raft.Storage;
using Voron;

namespace Rhino.Raft
{
	public class RaftEngine
	{
		private readonly CancellationToken _cancellationToken;
		public TextWriter DebugLog { get; set; }
		public ITransport Transport { get; set; }
		public TimeSpan HeartbeatTimeout { get; set; }
		public IRaftStateMachine StateMachine { get; set; }
		public IEnumerable<string> AllVotingPeers { get; set; }
		public IEnumerable<string> AllPeers { get; set; }
		public string Name { get; set; }
		public PersistentState PersistentState { get; set; }

		public ICommandSerializer CommandSerializer { get; set; }

		public string CurrentLeader { get; set; }
		public long CommitIndex { get; set; }
		public TimeSpan ElectionTimeout { get; set; }
		public int QuorumSize { get; set; }

		public RaftEngineState State { get; private set; }

		public AbstractRaftStateBehavior _currentBehavior;

		public int MaxEntriesPerRequest { get; set; }

		public bool _isRunning;

		public Task _eventLoopTask;

		public RaftEngine(string name, StorageEnvironmentOptions options, ITransport transport, IRaftStateMachine stateMachine, CancellationToken cancellationToken)
		{
			_cancellationToken = cancellationToken;
			if (transport == null) throw new ArgumentNullException("transport");
			if (stateMachine == null) throw new ArgumentNullException("stateMachine");
			if (String.IsNullOrWhiteSpace(name)) throw new ArgumentNullException("name");

			HeartbeatTimeout = Default.HeartbeatTimeout;
			ElectionTimeout = Default.ElectionTimeout;
			MaxEntriesPerRequest = Default.MaxEntriesPerRequest;
			Name = name;
			PersistentState = new PersistentState(options,cancellationToken);
			Transport = transport;
			StateMachine = stateMachine;
			State = RaftEngineState.Follower;			
			CommandSerializer = new JsonCommandSerializer();
			_currentBehavior = new FollowerStateBehavior(this);
			_isRunning = true;

			_eventLoopTask = Task.Run(() => EventLoop());
		}

		protected void EventLoop()
		{
			while (_isRunning)
			{
				_cancellationToken.ThrowIfCancellationRequested();
				_currentBehavior.RunOnce();
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
			State = state;
			switch (state)
			{
					case RaftEngineState.Follower:
						_currentBehavior = new FollowerStateBehavior(this);
					break;
					case RaftEngineState.Candidate:
						_currentBehavior = new CandidateStateBehavior(this);
					break;
					case RaftEngineState.Leader:
						_currentBehavior = new LeaderStateBehavior(this);
					break;
			}
		}

		internal bool LogIsUpToDate(long lastLogTerm, long lastLogIndex)
		{
			// Raft paper 5.4.1
			var lastLogEntry = PersistentState.LastLogEntry();
			if (lastLogEntry.Term < lastLogTerm)
				return true;
			return lastLogEntry.Index <= lastLogIndex;
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

			DebugLog.WriteLine("Calling an election in term {0}", PersistentState.CurrentTerm);

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

		internal void FireAndForgetCommand(Command cmd)
		{
			if (State != RaftEngineState.Leader)
				throw new InvalidOperationException("Only leaders can accept commands");
			Transport.Send(cmd);
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