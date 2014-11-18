// -----------------------------------------------------------------------
//  <copyright file="RaftEngine.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
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
		private readonly CancellationTokenSource _eventLoopCancellationTokenSource;
		private readonly ManualResetEventSlim _leaderSelectedEvent = new ManualResetEventSlim();

		private Topology _currentTopology;

		public DebugWriter DebugLog { get; set; }
		public ITransport Transport { get; set; }
		public IRaftStateMachine StateMachine { get; set; }

		public IEnumerable<string> AllVotingNodes
		{
			get
			{
				return _currentTopology.AllVotingNodes;
			}
		}

		public bool ContainedInAllVotingNodes(string node)
		{
			return _currentTopology.AllVotingNodes.Contains(node);
		}

		public int MaxLogLengthBeforeCompaction { get; set; }

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

				DebugLog.Write("Setting CurrentLeader: {0}", value);
				_currentLeader = value;


				if (value == null)
					_leaderSelectedEvent.Reset();
				else
				{
					_leaderSelectedEvent.Set();
				}
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

		public RaftEngineState State
		{
			get
			{
				var behavior = StateBehavior;
				if (behavior == null)
					return RaftEngineState.None;
				return behavior.State;
			}

		}

		public int MaxEntriesPerRequest { get; set; }

		private readonly Task _eventLoopTask;

		private long _commitIndex;
		private string _currentLeader;

		private Task _snapshottingTask;
		private Task _changingTopology;

		private AbstractRaftStateBehavior StateBehavior { get; set; }

		/// <summary>
		/// can be heartbeat timeout or election timeout - depends on the state behavior
		/// </summary>
		public int MessageTimeout { get; set; }

		public CancellationToken CancellationToken { get { return _eventLoopCancellationTokenSource.Token; } }

		public event Action<RaftEngineState> StateChanged;
		public event Action<Command> CommitApplied;

		public event Action ElectionStarted;
		public event Action StateTimeout;
		public event Action<LogEntry[]> EntriesAppended;
		public event Action<long, long> CommitIndexChanged;
		public event Action ElectedAsLeader;

		public event Action<TopologyChangeCommand> TopologyChangeFinished;
		public event Action TopologyChangeStarted;

		public event Action SnapshotCreationStarted;
		public event Action SnapshotCreationEnded;

		public event Action SnapshotInstallationStarted;
		public event Action SnapshotInstallationEnded;

		public event Action<Exception> SnapshotCreationError;

		/// <summary>
		/// will fire each time event loop of the node will process events and send response messages
		/// </summary>
		public event Action EventsProcessed;

		public RaftEngine(RaftEngineOptions raftEngineOptions)
		{
			Debug.Assert(raftEngineOptions.Stopwatch != null);
			DebugLog = new DebugWriter(raftEngineOptions.Name, raftEngineOptions.Stopwatch);

			CommandCommitTimeout = raftEngineOptions.CommandCommitTimeout;
			MessageTimeout = raftEngineOptions.MessageTimeout;

			_eventLoopCancellationTokenSource = new CancellationTokenSource();

			MaxEntriesPerRequest = Default.MaxEntriesPerRequest;
			Name = raftEngineOptions.Name;
			PersistentState = new PersistentState(raftEngineOptions.Options, _eventLoopCancellationTokenSource.Token)
			{
				CommandSerializer = new JsonCommandSerializer()
			};

			_currentTopology = PersistentState.GetCurrentTopology();

			if (raftEngineOptions.ForceNewTopology ||
				(_currentTopology.AllVotingNodes.Count == 0))
			{
				_currentTopology = new Topology(raftEngineOptions.AllVotingNodes ?? new[] { Name });
				PersistentState.SetCurrentTopology(_currentTopology, 0);
			}

			MaxLogLengthBeforeCompaction = raftEngineOptions.MaxLogLengthBeforeCompaction;

			//warm up to make sure that the serializer don't take too long and force election timeout
			PersistentState.CommandSerializer.Serialize(new NopCommand());

			Transport = raftEngineOptions.Transport;
			StateMachine = raftEngineOptions.StateMachine;

			var thereAreOthersInTheCluster = AllVotingNodes.Any(n => !n.Equals(Name, StringComparison.OrdinalIgnoreCase));
			if (thereAreOthersInTheCluster == false)
			{
				SetState(RaftEngineState.Leader);
				PersistentState.UpdateTermTo(PersistentState.CurrentTerm + 1);// restart means new term
			}
			else
			{
				SetState(RaftEngineState.Follower);
			}

			_eventLoopTask = Task.Run(() => EventLoop());
		}

		protected void EventLoop()
		{
			while (_eventLoopCancellationTokenSource.IsCancellationRequested == false)
			{
				try
				{
					MessageEnvelope message;
					var behavior = StateBehavior;
					var lastHeartBeat = (int)(DateTime.UtcNow - behavior.LastHeartbeatTime).TotalMilliseconds;
					var timeout = behavior.Timeout - lastHeartBeat;
					var hasMessage = Transport.TryReceiveMessage(Name, timeout, _eventLoopCancellationTokenSource.Token, out message);
					if (_eventLoopCancellationTokenSource.IsCancellationRequested)
						break;

					if (hasMessage == false)
					{
						if (State != RaftEngineState.Leader)
							DebugLog.Write("State {0} timeout ({1:#,#;;0} ms).", State, behavior.Timeout);
						behavior.HandleTimeout();
						OnStateTimeout();
						continue;
					}
					DebugLog.Write("State {0} message {1}", State, 
						message.Message is BaseMessage ? JsonConvert.SerializeObject(message.Message) : message.Message
						);

					behavior.HandleMessage(message);
				}
				catch (OperationCanceledException)
				{
					break;
				}
			}
		}

		internal void UpdateCurrentTerm(long term, string leader)
		{
			PersistentState.UpdateTermTo(term);
			SetState(RaftEngineState.Follower);
			DebugLog.Write("UpdateCurrentTerm() setting new leader : {0}", leader ?? "no leader currently");
			CurrentLeader = leader;
		}

		internal void SetState(RaftEngineState state)
		{
			if (state == State)
				return;

			if (State == RaftEngineState.Leader)
				_leaderSelectedEvent.Reset();

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
					case RaftEngineState.SnapshotInstallation:
						StateBehavior = new SnapshotInstallationStateBehavior(this);
						break;
					case RaftEngineState.Leader:
						StateBehavior = new LeaderStateBehavior(this);
						CurrentLeader = Name;
						OnElectedAsLeader();
						break;
					case RaftEngineState.None:
						_eventLoopCancellationTokenSource.Cancel(); //stop event loop						
						break;
					default:
						throw new ArgumentOutOfRangeException(state.ToString());
				}

				Debug.Assert(StateBehavior != null, "StateBehavior != null");

				OnStateChanged(state);
			}
		}

		public Task RemoveFromClusterAsync(string node)
		{
			if (_currentTopology.AllVotingNodes.Contains(node) == false)
				throw new InvalidOperationException("Node " + node + " was not found in the cluster");

			var requestedTopology = _currentTopology.CloneAndRemove(node);
			DebugLog.Write("RemoveFromClusterAsync, requestedTopology:{0}", requestedTopology.AllVotingNodes.Aggregate(String.Empty, (total, curr) => total + ", " + curr));
			return ModifyTopology(requestedTopology);
		}

		public Task AddToClusterAsync(string node)
		{
			if (_currentTopology.AllVotingNodes.Contains(node))
				throw new InvalidOperationException("Node " + node + " is already in the cluster");

			var requestedTopology = _currentTopology.CloneAndAdd(node);
			DebugLog.Write("AddToClusterClusterAsync, requestedTopology:{0}", requestedTopology.AllVotingNodes.Aggregate(String.Empty, (total, curr) => total + ", " + curr));
			return ModifyTopology(requestedTopology);
		}

		private Task ModifyTopology(Topology requested)
		{
			var tcc = new TopologyChangeCommand
				{
					Completion = new TaskCompletionSource<object>(),
					Requested = requested,
					BufferCommand = false,
				};

			if (Interlocked.CompareExchange(ref _changingTopology, tcc.Completion.Task, null) != null)
				throw new InvalidOperationException("Cannot change the cluster topology while another topology change is in progress");

			try
			{
				AppendCommand(tcc);
				DebugLog.Write("Topology change started (TopologyChangeCommand committed to the log)");
				TopologyChangeStarting(tcc);
				return tcc.Completion.Task;
			}
			catch (Exception)
			{
				Interlocked.Exchange(ref _changingTopology, null);
				throw;
			}
		}

		public Topology CurrentTopology
		{
			get { return _currentTopology; }
		}

		internal bool LogIsUpToDate(long lastLogTerm, long lastLogIndex)
		{
			// Raft paper 5.4.1
			var lastLogEntry = PersistentState.LastLogEntry();

			if (lastLogEntry.Term < lastLogTerm)
				return true;
			return lastLogEntry.Index <= lastLogIndex;
		}

		public void WaitForLeader()
		{
			_leaderSelectedEvent.Wait(CancellationToken);
		}

		public void AppendCommand(Command command)
		{
			if (command == null) throw new ArgumentNullException("command");

			var leaderStateBehavior = StateBehavior as LeaderStateBehavior;
			if (leaderStateBehavior == null)
				throw new InvalidOperationException("Command can be appended only on leader node. Leader node name is " +
													(CurrentLeader ?? "(no node leader yet)") + ", node behavior type is " +
													StateBehavior.GetType().Name);

			leaderStateBehavior.AppendCommand(command);
		}

		public void ApplyCommits(long from, long to)
		{
			Debug.Assert(to >= from, String.Format("assert to ({0}) >= from ({1})", to.ToString(CultureInfo.InvariantCulture), from.ToString(CultureInfo.InvariantCulture)));
			foreach (var entry in PersistentState.LogEntriesAfter(from, to))
			{
				try
				{
					var oldCommitIndex = CommitIndex;
					var command = PersistentState.CommandSerializer.Deserialize(entry.Data);

					var sysCommand = command is NopCommand || command is TopologyChangeCommand;

					if(sysCommand == false)
						StateMachine.Apply(entry, command);

					CommitIndex = entry.Index;
					DebugLog.Write("ApplyCommits --> CommitIndex changed to {0}", entry.Index);

					var tcc = command as TopologyChangeCommand;
					if (tcc != null)
					{
						DebugLog.Write("ApplyCommits for TopologyChangedCommand,tcc.Requested.AllVotingPeers = {0}, Name = {1}",
							String.Join(",", tcc.Requested.AllVotingNodes), Name);
						CommitTopologyChange(tcc);
					}

					OnCommitIndexChanged(oldCommitIndex, CommitIndex);
					OnCommitApplied(command);
				}
				catch (Exception e)
				{
					DebugLog.Write("Failed to apply commit. {0}", e);
					throw;
				}
			}

			if (StateMachine.SupportSnapshots == false)
				return;

			var commitedEntriesCount = PersistentState.GetCommitedEntriesCount(to);
			if (commitedEntriesCount >= MaxLogLengthBeforeCompaction)
			{
				SnapshotAndTruncateLog(to);
			}
		}

		private void SnapshotAndTruncateLog(long to)
		{
			var task = new Task(() =>
			{
				OnSnapshotCreationStarted();
				try
				{
					var currentTerm = PersistentState.CurrentTerm;
					StateMachine.CreateSnapshot(to, currentTerm);
					PersistentState.MarkSnapshotFor(to, currentTerm,
						MaxLogLengthBeforeCompaction - (MaxLogLengthBeforeCompaction / 8));

					OnSnapshotCreationEnded();
				}
				catch (Exception e)
				{
					DebugLog.Write("Failed to create snapshot because {0}", e);
					OnSnapshotCreationError(e);
				}
			});

			if (Interlocked.CompareExchange(ref _snapshottingTask, task, null) != null)
				return;
			task.Start();
		}


		private void CommitTopologyChange(TopologyChangeCommand tcc)
		{
			Interlocked.Exchange(ref _changingTopology, null);
			var shouldRemainInTopology = tcc.Requested.AllVotingNodes.Contains(Name);
			if (shouldRemainInTopology == false)
			{
				DebugLog.Write("@@@ This node is being removed from topology, emptying its AllVotingNodes list and settings its state to None (stopping event loop)");
				Interlocked.Exchange(ref _changingTopology, null);
				CurrentLeader = null;

				SetState(RaftEngineState.None);
			}
			else
			{
				if (_currentTopology.AllVotingNodes.Contains(CurrentLeader) == false)
				{
					CurrentLeader = null;
				}

				DebugLog.Write("@@@ Finished applying new topology. New AllVotingNodes: {0}", string.Join(",", _currentTopology.AllVotingNodes));
			}

			OnTopologyChanged(tcc);
		}

		internal void AnnounceCandidacy(bool firstTime)
		{
			var term = PersistentState.CurrentTerm;
			if (firstTime)
				term++; // we increment the term only for the trial election, then reuse the same term in the real election
			PersistentState.UpdateTermAndVoteFor(Name, term);
			CurrentLeader = null;

			DebugLog.Write("Calling for {0} election in term {1}", 
				firstTime ? "a trial" : "an",
				PersistentState.CurrentTerm);

			var lastLogEntry = PersistentState.LastLogEntry();
			var rvr = new RequestVoteRequest
			{
				CandidateId = Name,
				LastLogIndex = lastLogEntry.Index,
				LastLogTerm = lastLogEntry.Term,
				Term = PersistentState.CurrentTerm,
				From = Name,
				TrialOnly = firstTime
			};

			var allVotingNodes = AllVotingNodes;

			//dont't send to yourself the message
			foreach (var votingPeer in allVotingNodes.Where(node =>
											!node.Equals(Name, StringComparison.InvariantCultureIgnoreCase)))
			{
				Transport.Send(votingPeer, rvr);
			}

			OnCandidacyAnnounced();
		}

		public void Dispose()
		{
			_eventLoopCancellationTokenSource.Cancel();
			_eventLoopTask.Wait(500);

			PersistentState.Dispose();

		}

		protected virtual void OnCandidacyAnnounced()
		{
			var handler = ElectionStarted;
			if (handler != null)
			{
				try
				{
					handler();
				}
				catch (Exception e)
				{
					DebugLog.Write("Error on ElectionStarted event: " + e);
				}
			}
		}

		protected virtual void OnStateChanged(RaftEngineState state)
		{
			var handler = StateChanged;
			if (handler != null)
			{
				try
				{
					handler(state);
				}
				catch (Exception e)
				{
					DebugLog.Write("Error on StateChanged event: " + e);
				}
			}
		}

		protected virtual void OnStateTimeout()
		{
			var handler = StateTimeout;
			if (handler != null)
			{
				try
				{
					handler();
				}
				catch (Exception e)
				{
					DebugLog.Write("Error on StateTimeout event: " + e);
				}
			}
		}

		internal virtual void OnEntriesAppended(LogEntry[] logEntries)
		{
			var handler = EntriesAppended;
			if (handler != null)
			{
				try
				{
					handler(logEntries);
				}
				catch (Exception e)
				{
					DebugLog.Write("Error on EntriesAppended event: " + e);
				}
			}
		}

		protected virtual void OnCommitIndexChanged(long oldCommitIndex, long newCommitIndex)
		{
			var handler = CommitIndexChanged;
			if (handler != null)
			{
				try
				{
					handler(oldCommitIndex, newCommitIndex);
				}
				catch (Exception e)
				{
					DebugLog.Write("Error on CommitIndexChanged event: " + e);
				}
			}
		}

		protected virtual void OnElectedAsLeader()
		{
			var handler = ElectedAsLeader;
			if (handler != null)
			{
				try
				{
					handler();
				}
				catch (Exception e)
				{
					DebugLog.Write("Error on ElectedAsLeader event: " + e);
				}
			}
		}

		protected virtual void OnTopologyChanged(TopologyChangeCommand cmd)
		{
			var handler = TopologyChangeFinished;
			if (handler != null)
			{
				try
				{
					handler(cmd);
				}
				catch (Exception e)
				{
					DebugLog.Write("Error on TopologyChangeFinished event: " + e);
				}
			}
		}

		protected virtual void OnCommitApplied(Command cmd)
		{
			var handler = CommitApplied;
			if (handler != null)
			{
				try
				{
					handler(cmd);
				}
				catch (Exception e)
				{
					DebugLog.Write("Error on CommitApplied event: " + e);
				}
			}
		}

		internal virtual void OnTopologyChangeStarted(TopologyChangeCommand tcc)
		{
			var handler = TopologyChangeStarted;
			if (handler != null)
			{
				try
				{
					handler();
				}
				catch (Exception e)
				{
					DebugLog.Write("Error on TopologyChangeStarted event: " + e);
				}
			}
		}

		protected virtual void OnSnapshotCreationStarted()
		{
			var handler = SnapshotCreationStarted;
			if (handler != null)
			{
				try
				{
					handler();
				}
				catch (Exception e)
				{
					DebugLog.Write("Error on SnapshotCreationStarted event: " + e);
				}
			}
		}

		protected virtual void OnSnapshotCreationEnded()
		{
			var handler = SnapshotCreationEnded;
			if (handler != null)
			{
				try
				{
					handler();
				}
				catch (Exception e)
				{
					DebugLog.Write("Error on SnapshotCreationEnded event: " + e);
				}
			}
		}

		protected virtual void OnSnapshotCreationError(Exception e)
		{
			var handler = SnapshotCreationError;
			if (handler != null)
			{
				try
				{
					handler(e);
				}
				catch (Exception ex)
				{
					DebugLog.Write("Error on SnapshotCreationError event: " + ex);
				}
			}
		}

		internal virtual void OnEventsProcessed()
		{
			var handler = EventsProcessed;
			if (handler != null)
			{
				try
				{
					handler();
				}
				catch (Exception e)
				{
					DebugLog.Write("Error on EventsProcessed event: " + e);
				}
			}
		}

		internal virtual void OnSnapshotInstallationStarted()
		{
			var handler = SnapshotInstallationStarted;
			if (handler != null)
			{
				try
				{
					handler();
				}
				catch (Exception e)
				{
					DebugLog.Write("Error on SnapshotInstallationStarted event: " + e);
				}
			}
		}

		internal virtual void OnSnapshotInstallationEnded(long snapshotTerm)
		{
			var handler = SnapshotInstallationEnded;
			if (handler != null)
			{
				try
				{
					handler();
				}
				catch (Exception e)
				{
					DebugLog.Write("Error on SnapshotInstallationEnded event: " + e);
				}
			}
		}

		public override string ToString()
		{
			return string.Format("Name: {0}", Name);
		}

		internal void TopologyChangeStarting(TopologyChangeCommand tcc)
		{
			_currentTopology = tcc.Requested;
			Interlocked.Exchange(ref _changingTopology, new TaskCompletionSource<object>().Task);
			OnTopologyChangeStarted(tcc);
		}

		internal void RevertTopologyTo(string[] previousPeers)
		{
			Interlocked.Exchange(ref _changingTopology, null);
			_currentTopology = new Topology(previousPeers);
			OnTopologyChanged(new TopologyChangeCommand
			{
				Requested = new Topology(previousPeers)
			});
		}
	}
}