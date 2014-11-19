using System;
using System.IO;
using System.Linq;
using NLog;
using Rhino.Raft.Commands;
using Rhino.Raft.Messages;


namespace Rhino.Raft.Behaviors
{
	public abstract class AbstractRaftStateBehavior : IDisposable
	{
		protected readonly RaftEngine Engine;
		public int Timeout { get; set; }
		public abstract RaftEngineState State { get; }

		public DateTime LastHeartbeatTime { get; set; }

		public void HandleMessage(MessageContext context)
		{
			bool asyncMessageHandling = false;
			try
			{
				RequestVoteRequest requestVoteRequest;
				RequestVoteResponse requestVoteResponse;
				AppendEntriesResponse appendEntriesResponse;
				AppendEntriesRequest appendEntriesRequest;
				InstallSnapshotRequest installSnapshotRequest;
				InstallSnapshotResponse installSnapshotResponse;
				CanInstallSnapshotRequest canInstallSnapshotRequest;
				CanInstallSnapshotResponse canInstallSnapshotResponse;
				TimeoutNowRequest timeoutNowRequest;
				Action action;

				if (context.Message is NothingToDo)
					return;

				if (TryCastMessage(context.Message, out requestVoteRequest))
				{
					var reply = Handle(requestVoteRequest);
					context.Reply(reply);
				}
				else if (TryCastMessage(context.Message, out appendEntriesResponse))
				{
					Handle(appendEntriesResponse);
				}
				else if (TryCastMessage(context.Message, out appendEntriesRequest))
				{
					var reply = Handle(appendEntriesRequest);
					context.Reply(reply);
				}
				else if (TryCastMessage(context.Message, out requestVoteResponse))
				{
					Handle(requestVoteResponse);
				}
				else if (TryCastMessage(context.Message, out canInstallSnapshotRequest))
				{
					var reply = Handle(canInstallSnapshotRequest);
					context.Reply(reply);
				}
				else if (TryCastMessage(context.Message, out installSnapshotRequest))
				{
					var reply = Handle(context, installSnapshotRequest, context.Stream);
					if (reply != null)
						context.Reply(reply);
					else
						asyncMessageHandling = true;
				}
				else if (TryCastMessage(context.Message, out canInstallSnapshotResponse))
				{
					Handle(canInstallSnapshotResponse);
				}
				else if (TryCastMessage(context.Message, out installSnapshotResponse))
				{
					Handle(installSnapshotResponse);
				}
				else if (TryCastMessage(context.Message, out timeoutNowRequest))
				{
					Handle(timeoutNowRequest);
				}
				else if (TryCastMessage(context.Message, out action))
				{
					action();
				}

				Engine.OnEventsProcessed();
			}
			catch (Exception e)
			{
				context.Error(e);
			}
			finally
			{
				if (asyncMessageHandling == false)
					context.Done();
			}
		}

		public void Handle(TimeoutNowRequest req)
		{
			if (req.Term < Engine.PersistentState.CurrentTerm)
			{
				_log.Info("Got timeout now request from an older term, ignoring");
				return;
			}
			if (req.From != Engine.CurrentLeader)
			{
				_log.Info("Got timeout now request from {0}, who isn't the current leader, ignoring.",
					req.From);
				return;
			}

			_log.Info("Got timeout now request from the leader, timing out and forcing immediate election");
			Engine.SetState(RaftEngineState.CandidateByRequest);
		}

		public virtual InstallSnapshotResponse Handle(MessageContext context, InstallSnapshotRequest req, Stream stream)
		{
			stream.Dispose();
			return new InstallSnapshotResponse
			{
				Success = false,
				Message = "Cannot install snapshot from state " + State,
				CurrentTerm = Engine.PersistentState.CurrentTerm,
				From = Engine.Name,
				LastLogIndex = Engine.PersistentState.LastLogEntry().Index
			};
		}

		public virtual void Handle(RequestVoteResponse resp)
		{
			//do nothing, irrelevant here
		}

		protected static bool TryCastMessage<T>(object abstractMessage, out T typedMessage)
			where T : class
		{
			typedMessage = abstractMessage as T;
			return typedMessage != null;
		}

		public abstract void HandleTimeout();

		protected Logger _log;

		protected AbstractRaftStateBehavior(RaftEngine engine)
		{
			Engine = engine;
			LastHeartbeatTime = DateTime.UtcNow;
			_log = LogManager.GetLogger(GetType().Name + "." + engine.Name);
		}

		public RequestVoteResponse Handle(RequestVoteRequest req)
		{
			//disregard RequestVoteRequest if this node receives regular heartbeats and the leader is known
			// Raft paper section 6 (cluster membership changes), this apply only if we are a follower, because
			// candidate and leaders both generate their own heartbeat messages
			var timeSinceLastHeartbeat = (int)(DateTime.UtcNow - LastHeartbeatTime).TotalMilliseconds;

			if (State == RaftEngineState.Follower && req.ForcedElection == false &&
				(timeSinceLastHeartbeat < (Timeout / 2)) && Engine.CurrentLeader != null)
			{
				_log.Info("Received RequestVoteRequest from a node within election timeout while leader exists, rejecting");
				return new RequestVoteResponse
				{
					VoteGranted = false,
					CurrentTerm = Engine.PersistentState.CurrentTerm,
					VoteTerm = req.Term,
					Message = "I currently have a leader and I am receiving heartbeats within election timeout.",
					From = Engine.Name,
					TrialOnly = req.TrialOnly
				};
			}

			if (Engine.ContainedInAllVotingNodes(req.From) == false)
			{
				_log.Info("Received RequestVoteRequest from a node that isn't a member in the cluster: {0}, rejecting", req.From);
				return new RequestVoteResponse
				{
					VoteGranted = false,
					CurrentTerm = Engine.PersistentState.CurrentTerm,
					VoteTerm = req.Term,
					Message = "You are not a memeber in my cluster, and cannot be a leader",
					From = Engine.Name,
					TrialOnly = req.TrialOnly
				};
			}

			_log.Debug("Received RequestVoteRequest, req.CandidateId = {0}, term = {1}", req.From, req.Term);

			if (req.Term < Engine.PersistentState.CurrentTerm)
			{
				var msg = string.Format("Rejecting request vote because term {0} is lower than current term {1}",
					req.Term, Engine.PersistentState.CurrentTerm);
				_log.Info(msg);
				return new RequestVoteResponse
				{
					VoteGranted = false,
					CurrentTerm = Engine.PersistentState.CurrentTerm,
					VoteTerm = req.Term,
					Message = msg,
					From = Engine.Name,
					TrialOnly = req.TrialOnly
				};
			}

			if (req.Term > Engine.PersistentState.CurrentTerm && req.TrialOnly == false)
			{
				Engine.UpdateCurrentTerm(req.Term, null);
			}

			if (Engine.PersistentState.VotedFor != null && Engine.PersistentState.VotedFor != req.From &&
				Engine.PersistentState.VotedForTerm <= req.Term)
			{
				var msg = string.Format("Rejecting request vote because already voted for {0} in term {1}",
					Engine.PersistentState.VotedFor, req.Term);

				_log.Info(msg);
				return new RequestVoteResponse
				{
					VoteGranted = false,
					CurrentTerm = Engine.PersistentState.CurrentTerm,
					VoteTerm = req.Term,
					Message = msg,
					From = Engine.Name,
					TrialOnly = req.TrialOnly
				};
			}

			if (Engine.LogIsUpToDate(req.LastLogTerm, req.LastLogIndex) == false)
			{
				var msg = string.Format("Rejecting request vote because remote log for {0} in not up to date.", req.From);
				_log.Info(msg);
				return new RequestVoteResponse

				{
					VoteGranted = false,
					CurrentTerm = Engine.PersistentState.CurrentTerm,
					VoteTerm = req.Term,
					Message = msg,
					From = Engine.Name,
					TrialOnly = req.TrialOnly
				};
			}
			// we said we would be voting for this guy, so we can give it a full election timeout, 
			// by treating this as a heart beat. This means we won't be timing out ourselves and trying
			// to become the leader
			LastHeartbeatTime = DateTime.UtcNow;

			if (req.TrialOnly == false)
			{
				_log.Info("Recording vote for candidate = {0}", req.From);
				Engine.PersistentState.RecordVoteFor(req.From, req.Term);
			}
			else
			{
				_log.Info("Voted for candidate = {0} in trial election for term {1}", req.From, req.Term);
			}
			return new RequestVoteResponse
			{
				VoteGranted = true,
				CurrentTerm = Engine.PersistentState.CurrentTerm,
				VoteTerm = req.Term,
				Message = "Vote granted",
				From = Engine.Name,
				TrialOnly = req.TrialOnly
			};
		}

		public virtual void Handle(CanInstallSnapshotResponse resp)
		{
			//irrelevant here, so doing nothing (used only in LeaderStateBehavior)
		}

		public virtual void Handle(InstallSnapshotResponse resp)
		{
			//irrelevant here, so doing nothing (used only in LeaderStateBehavior)
		}

		public virtual CanInstallSnapshotResponse Handle(CanInstallSnapshotRequest req)
		{
			var lastLogEntry = Engine.PersistentState.LastLogEntry();

			var index = lastLogEntry.Index;
			if (req.Term <= Engine.PersistentState.CurrentTerm && req.Index <= index)
			{
				return new CanInstallSnapshotResponse
				{
					From = Engine.Name,
					IsCurrentlyInstalling = false,
					Message = String.Format("Term or Index do not match the ones on this node. Cannot install snapshot. (CurrentTerm = {0}, req.Term = {1}, LastLogEntry index = {2}, req.Index = {3}",
						Engine.PersistentState.CurrentTerm, req.Term, index, req.Index),
					Success = false,
					Index = index,
					Term = Engine.PersistentState.CurrentTerm
				};
			}

			Engine.SetState(RaftEngineState.SnapshotInstallation);

			return new CanInstallSnapshotResponse
			{
				From = Engine.Name,
				IsCurrentlyInstalling = false,
				Message = "Everything ok, go ahead, install the snapshot!",
				Success = true
			};
		}

		public virtual void Handle(AppendEntriesResponse resp)
		{
			// not a leader, no idea what to do with this. Probably an old
			// message from when we were a leader, ignoring.			
		}

		public virtual AppendEntriesResponse Handle(AppendEntriesRequest req)
		{
			if (req.Term < Engine.PersistentState.CurrentTerm)
			{
				var msg = string.Format(
					"Rejecting append entries because msg term {0} is lower then current term: {1}",
					req.Term, Engine.PersistentState.CurrentTerm);

				_log.Info(msg);

				return new AppendEntriesResponse
				{
					Success = false,
					CurrentTerm = Engine.PersistentState.CurrentTerm,
					LastLogIndex = Engine.PersistentState.LastLogEntry().Index,
					LeaderId = Engine.CurrentLeader,
					Message = msg,
					From = Engine.Name,
				};
			}

			if (req.Term > Engine.PersistentState.CurrentTerm)
			{
				Engine.UpdateCurrentTerm(req.Term, req.From);
			}

			if (Engine.CurrentLeader == null || req.From.Equals(Engine.CurrentLeader) == false)
			{
				Engine.CurrentLeader = req.From;
				Engine.SetState(RaftEngineState.Follower);
			}

			var prevTerm = Engine.PersistentState.TermFor(req.PrevLogIndex) ?? 0;
			if (prevTerm != req.PrevLogTerm)
			{
				var msg = string.Format(
					"Rejecting append entries because msg previous term {0} is not the same as the persisted current term {1} at log index {2}",
					req.PrevLogTerm, prevTerm, req.PrevLogIndex);
				_log.Info(msg);
				return new AppendEntriesResponse
				{
					Success = false,
					CurrentTerm = Engine.PersistentState.CurrentTerm,
					LastLogIndex = Engine.PersistentState.LastLogEntry().Index,
					Message = msg,
					LeaderId = Engine.CurrentLeader,
					From = Engine.Name
				};
			}

			LastHeartbeatTime = DateTime.UtcNow;
			if (req.Entries.Length > 0)
			{
				_log.Debug("Appending log (persistant state), entries count: {0} (node state = {1})", req.Entries.Length,
					Engine.State);

				// if we get an append entries, we are good, because that means that there is a leader (one has to be manually seeded)
				// and now that we go that notice from him, we can now also become leaders
				Engine.PersistentState.MarkAsLeaderPotential();

				// if is possible that we'll get the same event multiple times (for example, if we took longer than a heartbeat
				// to process a message). In this case, our log already have the entries in question, and it would be a waste to
				// truncate the log and re-add them all the time. What we are doing here is to find the next match for index/term
				// values in our log and in the entries, and then skip over the duplicates.

				var skip = 0;

				for (int i = 0; i < req.Entries.Length; i++)
				{
					var termForEntry = Engine.PersistentState.TermFor(req.Entries[i].Index) ?? -1;
					if (termForEntry != req.Entries[i].Term)
						break;
					skip++;
				}

				if (skip != req.Entries.Length)
					Engine.PersistentState.AppendToLog(Engine, req.Entries.Skip(skip), req.PrevLogIndex + skip);

				var topologyChange = req.Entries.LastOrDefault(x => x.IsTopologyChange == true);

				// we consider the latest topology change to be in effect as soon as we see it, even before the 
				// it is committed, see raft spec section 6:
				//		a server always uses the latest conﬁguration in its log, 
				//		regardless of whether the entry is committed
				if (topologyChange != null)
				{
					var command = Engine.PersistentState.CommandSerializer.Deserialize(topologyChange.Data);
					var topologyChangeCommand = command as TopologyChangeCommand;
					if (topologyChangeCommand == null) //precaution,should never be true
						//if this is true --> it is a serious issue and should be fixed immediately!
						throw new InvalidOperationException(@"Log entry that is marked with IsTopologyChange should be of type TopologyChangeCommand.
															Instead, it is of type: " + command.GetType() + ". It is probably a bug!");

					_log.Info("Topology change started (TopologyChangeCommand committed to the log): {0}",
						topologyChangeCommand.Requested.AllVotingNodes);
					Engine.PersistentState.SetCurrentTopology(topologyChangeCommand.Requested, topologyChange.Index);
					Engine.TopologyChangeStarting(topologyChangeCommand);
				}
			}

			var lastIndex = req.Entries.Length == 0 ?
				Engine.PersistentState.LastLogEntry().Index :
				req.Entries[req.Entries.Length - 1].Index;
			try
			{
				if (req.LeaderCommit > Engine.CommitIndex)
				{
					CommitEntries(req.Entries, lastIndex, req.LeaderCommit);
				}

				return new AppendEntriesResponse
				{
					Success = true,
					CurrentTerm = Engine.PersistentState.CurrentTerm,
					LastLogIndex = Engine.PersistentState.LastLogEntry().Index,
					From = Engine.Name
				};
			}
			catch (Exception e)
			{
				return new AppendEntriesResponse
				{
					Success = false,
					CurrentTerm = Engine.PersistentState.CurrentTerm,
					LastLogIndex = Engine.PersistentState.LastLogEntry().Index,
					Message = "Failed to apply new entries. Reason: " + e,
					From = Engine.Name
				};
			}
		}

		protected void CommitEntries(LogEntry[] entries, long lastIndex, long leaderCommit)
		{
			var oldCommitIndex = Engine.CommitIndex + 1;
			Engine.CommitIndex = Math.Min(leaderCommit, lastIndex);
			Engine.ApplyCommits(oldCommitIndex, Engine.CommitIndex);
			
			Engine.OnEntriesAppended(entries);
		}

		public virtual void Dispose()
		{

		}
	}
}