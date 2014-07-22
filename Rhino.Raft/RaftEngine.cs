// -----------------------------------------------------------------------
//  <copyright file="RaftEngine.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Concurrent;
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
		public IRaftStateMachine StateMachine { get; set; }
		public IEnumerable<string> AllVotingPeers { get; set; }
		public IEnumerable<string> AllPeers { get; set; }
		public string Name { get; set; }
		public PersistentState PersistentState { get; set; }
		public ICommandSerializer CommandSerializer { get; set; }
		public string CurrentLeader { get; set; }
		public long CommitIndex { get; set; }
		public int QuorumSize { get; set; }

		public RaftEngineState State { get; private set; }

		public int MaxEntriesPerRequest { get; set; }

		private Task _eventLoopTask;

		private BlockingCollection<MessageEnvelope> _messages = new BlockingCollection<MessageEnvelope>();

		private AbstractRaftStateBehavior StateBehavior { get; set; }

		public RaftEngine(string name, StorageEnvironmentOptions options, ITransport transport, IRaftStateMachine stateMachine, CancellationToken cancellationToken)
		{
			_cancellationToken = cancellationToken;
			if (transport == null) throw new ArgumentNullException("transport");
			if (stateMachine == null) throw new ArgumentNullException("stateMachine");
			if (String.IsNullOrWhiteSpace(name)) throw new ArgumentNullException("name");

			MaxEntriesPerRequest = Default.MaxEntriesPerRequest;
			Name = name;
			PersistentState = new PersistentState(options, cancellationToken);
			Transport = transport;
			StateMachine = stateMachine;
			SetState(RaftEngineState.Follower);
			CommandSerializer = new JsonCommandSerializer();

			_eventLoopTask = Task.Run(() => EventLoop());
		}

		protected void EventLoop()
		{
			while (true)
			{
				_cancellationToken.ThrowIfCancellationRequested();
				MessageEnvelope item;
				var hasMessage = _messages.TryTake(out item, StateBehavior.Timeout, _cancellationToken);
				if (hasMessage == false)
				{
					StateBehavior.HandleTimeout();
					continue;
				}

				// dispatch message
				var appendEntriesRequest = item.Message as AppendEntriesRequest;
				if (appendEntriesRequest != null)
				{
					StateBehavior.Handle(item.Source, appendEntriesRequest);
					continue;
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