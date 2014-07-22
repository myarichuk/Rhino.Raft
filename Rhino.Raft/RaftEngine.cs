// -----------------------------------------------------------------------
//  <copyright file="RaftEngine.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Rhino.Raft.Behaviors;
using Rhino.Raft.Commands;
using Rhino.Raft.Interfaces;
using Rhino.Raft.Messages;
using Rhino.Raft.Storage;

namespace Rhino.Raft
{
	public class RaftEngine : IDisposable
	{
		private readonly CancellationTokenSource _cancellationToken;
		public TextWriter DebugLog { get; set; }
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

		public int QuorumSize { get; set; }

		public RaftEngineState State { get; private set; }

		public int MaxEntriesPerRequest { get; set; }

		private readonly Task _eventLoopTask;

		private readonly BlockingCollection<MessageEnvelope> _messages = new BlockingCollection<MessageEnvelope>();
		private long _commitIndex;

		private AbstractRaftStateBehavior StateBehavior { get; set; }

		public int ElectionTimeout { get; set; }
		public CancellationToken CancellationToken { get { return _cancellationToken.Token; } }

		public event Action<RaftEngineState> StateChanged;

		public RaftEngine(RaftEngineOptions raftEngineOptions)
		{
			AllPeers = raftEngineOptions.AllPeers ?? new List<string>();
			AllVotingPeers = new List<string>();
			CommandSerializer = new JsonCommandSerializer();

			_cancellationToken = new CancellationTokenSource();

			MaxEntriesPerRequest = Default.MaxEntriesPerRequest;
			Name = raftEngineOptions.Name;
			PersistentState = new PersistentState(raftEngineOptions.Options, _cancellationToken.Token);
			Transport = raftEngineOptions.Transport;
			StateMachine = raftEngineOptions.StateMachine;

			SetState(AllPeers.Any() ? RaftEngineState.Follower : RaftEngineState.Leader);

			DebugLog = Console.Out;

			_eventLoopTask = Task.Run(() => EventLoop());
		}

		protected void EventLoop()
		{
			while (_cancellationToken.IsCancellationRequested == false)
			{
				try
				{
					MessageEnvelope message;
					var hasMessage = _messages.TryTake(out message, StateBehavior.Timeout, _cancellationToken.Token);

					if (hasMessage == false)
					{
						StateBehavior.HandleTimeout();
						continue;
					}

					StateBehavior.HandleMessage(message);
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
						StateBehavior = new LeaderStateBehavior(this);
						break;
				}
			}

			OnStateChanged(state);
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

		public void Dispose()
		{
			_cancellationToken.Cancel();
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