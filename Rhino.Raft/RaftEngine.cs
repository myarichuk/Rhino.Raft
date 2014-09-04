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
using Voron.Util;

namespace Rhino.Raft
{
	public class RaftEngine : IDisposable
	{
		private readonly CancellationTokenSource _cancellationTokenSource;
		private readonly ManualResetEventSlim _leaderSelectedEvent = new ManualResetEventSlim();
		private readonly object _stateChangingSyncObject = new object();

		private Configuration _currentConfiguration;

		public DebugWriter DebugLog { get; set; }
		public ITransport Transport { get; set; }
		public IRaftStateMachine StateMachine { get; set; }

		public IEnumerable<string> AllVotingPeers
		{
			get { return _currentConfiguration.AllVotingPeers; }
			set
			{
				_currentConfiguration = new Configuration(_currentConfiguration.AllPeers,value);
				PersistentState.SetCurrentConfiguration(_currentConfiguration);
			}
		}

		public IEnumerable<string> AllPeers
		{
			get { return _currentConfiguration.AllPeers; }
			set
			{
				_currentConfiguration = new Configuration(value, _currentConfiguration.AllVotingPeers);
				PersistentState.SetCurrentConfiguration(_currentConfiguration);
			}
		}

		public string Name { get; set; }
		public PersistentState PersistentState { get; set; }

		public long CommandCommitTimeout { get; private set; }

		public string CurrentLeader
		{
			get
			{
				return _currentLeader;
			}
			set
			{
				if (_currentLeader == value)
					return;

				DebugLog.Write("Setting CurrentLeader: " + value);
				_currentLeader = value;
				if (_currentLeader == null)
					_leaderSelectedEvent.Reset();
				else
					_leaderSelectedEvent.Set();
			}
		}

		/// <summary>
		/// This is a thread safe operation, since this is being used by both the leader's message processing thread
		/// and the leader's heartbeat thread
		/// </summary>
		public long CommitIndex
		{
			get
			{
				return Thread.VolatileRead(ref _commitIndex);
			}
			set
			{
				Interlocked.Exchange(ref _commitIndex, value);
			}
		}

		public int QuorumSize
		{
			get
			{
				return ((AllVotingPeers.Count() + 1)/2) + 1;
			}
		}

		private RaftEngineState _state;

		public RaftEngineState State 
		{
			get
			{
				lock(_stateChangingSyncObject)
					return _state;
			}
			
		}

		public int MaxEntriesPerRequest { get; set; }

		private readonly Task _eventLoopTask;

		private long _commitIndex;
		private AbstractRaftStateBehavior _stateBehavior;
		private string _currentLeader;

		private AbstractRaftStateBehavior StateBehavior
		{
			get { return _stateBehavior; }
			set
			{
				_stateBehavior = value;
				_stateBehavior.EntriesAppended += OnEntriesAppended;
			}
		}

		/// <summary>
		/// can be heartbeat timeout or election timeout - depends on the state behavior
		/// </summary>
		public int MessageTimeout { get; set; }
		public CancellationToken CancellationToken { get { return _cancellationTokenSource.Token; } }
		
		public event Action<RaftEngineState> StateChanged;
		public event Action ElectionStarted;
		public event Action StateTimeout;
		public event Action<LogEntry[]> EntriesAppended;
		public event Action<long, long> CommitIndexChanged;
		public event Action ElectedAsLeader;
		public event Action TopologyChanged;

		public RaftEngine(RaftEngineOptions raftEngineOptions)
		{
			Debug.Assert(raftEngineOptions.Stopwatch != null);
			DebugLog = new DebugWriter(raftEngineOptions.Name, raftEngineOptions.Stopwatch);

			CommandCommitTimeout = raftEngineOptions.CommandCommitTimeout;
			MessageTimeout = raftEngineOptions.MessageTimeout;
			
			_cancellationTokenSource = new CancellationTokenSource();

			MaxEntriesPerRequest = Default.MaxEntriesPerRequest;
			Name = raftEngineOptions.Name;
			PersistentState = new PersistentState(raftEngineOptions.Options, _cancellationTokenSource.Token)
			{
				CommandSerializer = new JsonCommandSerializer()
			};

			if (raftEngineOptions.AllPeers != null || raftEngineOptions.AllPeers != null)
			{
// ReSharper disable ConstantNullCoalescingCondition
				_currentConfiguration = new Configuration(raftEngineOptions.AllPeers ?? new String[0], raftEngineOptions.AllVotingPeers ?? new String[0]);
// ReSharper restore ConstantNullCoalescingCondition
				PersistentState.SetCurrentConfiguration(_currentConfiguration);
			}
			else
				_currentConfiguration = PersistentState.GetCurrentConfiguration();

			//warm up
			PersistentState.CommandSerializer.Serialize(new NopCommand());

			Transport = raftEngineOptions.Transport;
			StateMachine = raftEngineOptions.StateMachine;

			SetState(AllPeers.Any() ? RaftEngineState.Follower : RaftEngineState.Leader);

			_eventLoopTask = Task.Run(() => EventLoop());
		}

		public void RemoveFromCluster()
		{
			DebugLog.Write("Removal of node {0} from cluster --> started", Name);
			_cancellationTokenSource.Cancel();
			DebugLog.Write("Removal of node {0} from cluster --> cancelling event loop on the node", Name);
			var oldConfig = PersistentState.GetCurrentConfiguration();

			var allVotingPeersWithoutThis = oldConfig.AllVotingPeers;
			var allPeersWithoutThis = oldConfig.AllPeers;
			var newConfig = new Configuration(allPeersWithoutThis, allVotingPeersWithoutThis);
			
			oldConfig = new Configuration(oldConfig.AllPeers.Concat(Name), oldConfig.AllVotingPeers.Concat(Name));
			
			_stateBehavior.PublishTopologyChanges(oldConfig, newConfig);
		}

		internal void SetCurrentConfiguration(Configuration configuration)
		{
			_currentConfiguration = configuration;
			PersistentState.SetCurrentConfiguration(_currentConfiguration);
			DebugLog.Write("SetCurrentConfiguration: {0}", String.Join(",", _currentConfiguration.AllPeers));
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
					if (_cancellationTokenSource.IsCancellationRequested)
						break;

					if (hasMessage == false)
					{
						DebugLog.Write("State {0} timeout ({1:#,#;;0} ms).", State, behavior.Timeout);
						behavior.HandleTimeout();
						OnStateTimeout();
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

		internal void UpdateCurrentTerm(long term,string leader)
		{
			PersistentState.UpdateTermTo(term);
			SetState(RaftEngineState.Follower);
			DebugLog.Write("UpdateCurrentTerm() setting new leader : {0}",leader ?? "no leader currently");
			CurrentLeader = leader;
		}

		internal void SetState(RaftEngineState state)
		{
			if (state == State)
				return;

			if (StateBehavior != null)
				StateBehavior.EntriesAppended -= OnEntriesAppended;

			var oldState = StateBehavior;

			lock (_stateChangingSyncObject)
			using (oldState)
			{
				_state = state;

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
						OnElectedAsLeader();
						break;
				}

// ReSharper disable once PossibleNullReferenceException
				if(oldState != null)
					StateBehavior.TopologyChanges = oldState.TopologyChanges;
				OnStateChanged(state);
			}
		}

		internal bool LogIsUpToDate(long lastLogTerm, long lastLogIndex)
		{
			// Raft paper 5.4.1
			var lastLogEntry = PersistentState.LastLogEntry() ?? new LogEntry();

			if (lastLogEntry.Term < lastLogTerm)
				return true;
			return lastLogEntry.Index <= lastLogIndex;
		}

		public Task WaitForLeader()
		{
			lock(_stateChangingSyncObject) //change to leader state makes _leaderSelectedEvent to be set, 
										   //so wait for state to finish changing
				return Task.Run(() => _leaderSelectedEvent.Wait(CancellationToken), CancellationToken);
		}

		public void AppendCommand(Command command)
		{
			if (command == null) throw new ArgumentNullException("command");

			//since event loop is on separate thread, and state change will occur on that separate thread,
			//make sure that state is finished changing before we attempt to append commands
			lock (_stateChangingSyncObject)
			{
				var leaderStateBehavior = StateBehavior as LeaderStateBehavior;
				if (leaderStateBehavior == null)
					throw new InvalidOperationException("Command can be appended only on leader node. Leader node name is " +
					                                    (CurrentLeader ?? "(no current node)") + ", node type is " + StateBehavior.GetType().Name);

				leaderStateBehavior.AppendCommand(command);
			}
		}

		public void ApplyCommits(long from, long to)
		{
			Debug.Assert(to >= from);

			foreach (var entry in PersistentState.LogEntriesAfter(from, to))
			{
				try
				{
					StateMachine.Apply(entry);
				}
				catch (InvalidOperationException e)
				{
					DebugLog.Write("Failed to apply commit. {0}",e);
					throw;
				}
			}
			var oldCommitIndex = CommitIndex;
			CommitIndex = to;
			DebugLog.Write("ApplyCommits --> CommitIndex changed to {0}", CommitIndex);
			OnCommitIndexChanged(oldCommitIndex,CommitIndex);
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

			OnCandidacyAnnounced();
		}

		public void Dispose()
		{
			_cancellationTokenSource.Cancel();
			_eventLoopTask.Wait(500);			

			PersistentState.Dispose();
		}

		protected virtual void OnCandidacyAnnounced()
		{
			var handler = ElectionStarted;
			if (handler != null) handler();
		}

		protected virtual void OnStateChanged(RaftEngineState state)
		{
			var handler = StateChanged;
			if (handler != null) handler(state);
		}

		protected virtual void OnStateTimeout()
		{
			var handler = StateTimeout;
			if (handler != null) handler();
		}

		protected virtual void OnEntriesAppended(LogEntry[] logEntries)
		{
			var handler = EntriesAppended;
			if (handler != null) handler(logEntries);
		}

		protected virtual void OnCommitIndexChanged(long oldCommitIndex,long newCommitIndex)
		{
			var handler = CommitIndexChanged;
			if (handler != null) handler(oldCommitIndex,newCommitIndex);
		}

		protected virtual void OnElectedAsLeader()
		{
			_leaderSelectedEvent.Set();
			var handler = ElectedAsLeader;
			if (handler != null) handler();
		}
		protected virtual void OnTopologyChanged()
		{
			var handler = TopologyChanged;
			if (handler != null) handler();
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