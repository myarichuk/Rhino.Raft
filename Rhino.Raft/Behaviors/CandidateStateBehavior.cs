// -----------------------------------------------------------------------
//  <copyright file="CandidateStateBehavior.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Linq;
using Rhino.Raft.Messages;

namespace Rhino.Raft.Behaviors
{
	public class CandidateStateBehavior : AbstractRaftStateBehavior
    {
		private readonly bool _forcedElection;
		private readonly HashSet<string> _votesForMyLeadership = new HashSet<string>();
		private readonly Random _random;
		private bool _wonTrialElection;

		public CandidateStateBehavior(RaftEngine engine, bool forcedElection) : base(engine)
		{
			_forcedElection = forcedElection;
			_wonTrialElection = forcedElection; 
			_random = new Random((int) (engine.Name.GetHashCode() + DateTime.UtcNow.Ticks));
			Timeout = _random.Next(engine.Options.MessageTimeout / 2, engine.Options.MessageTimeout);
			StartElection();
		}

		private void VoteForSelf()
		{
			_log.Info("Voting for myself in term {0}", Engine.PersistentState.CurrentTerm);
			Handle(Engine.Name,
				new RequestVoteResponse
				{
					CurrentTerm = Engine.PersistentState.CurrentTerm,
					VoteGranted = true,
					Message = String.Format("{0} -> Voting for myself", Engine.Name),
					From = Engine.Name,
					VoteTerm = Engine.PersistentState.CurrentTerm,
				});
		}

		public override void HandleTimeout()
	    {
			_log.Info("Timeout ({1:#,#;;0} ms) for elections in term {0}", Engine.PersistentState.CurrentTerm,
				  Timeout);

			Timeout = _random.Next(Engine.Options.MessageTimeout / 2, Engine.Options.MessageTimeout);
			_wonTrialElection = false;
			StartElection();
	    }

		private void StartElection()
		{
			LastHeartbeatTime = DateTime.UtcNow;
			_votesForMyLeadership.Clear();
			var term = Engine.PersistentState.CurrentTerm;
			Engine.PersistentState.RecordVoteFor(Engine.Name, term + 1);

			if (_wonTrialElection) // only in the real election, we increment the current term
				Engine.PersistentState.UpdateTermTo(Engine, Engine.PersistentState.CurrentTerm + 1);

			Engine.CurrentLeader = null;
			_log.Info("Calling for {0} election in term {1}",
				_wonTrialElection ? "an" : "a trial", Engine.PersistentState.CurrentTerm);

			var lastLogEntry = Engine.PersistentState.LastLogEntry();
			var rvr = new RequestVoteRequest
			{
				CandidateId = Engine.Name,
				LastLogIndex = lastLogEntry.Index,
				LastLogTerm = lastLogEntry.Term,
				Term = Engine.PersistentState.CurrentTerm,
				From = Engine.Name,
				TrialOnly = _wonTrialElection == false,
				ForcedElection = _forcedElection
			};

			var allVotingNodes = Engine.AllVotingNodes;

			//dont't send to yourself the message
			foreach (var votingPeer in allVotingNodes.Where(node =>
				!node.Equals(Engine.Name, StringComparison.InvariantCultureIgnoreCase)))
			{
				Engine.Transport.Send(votingPeer, rvr);
			}

			Engine.OnCandidacyAnnounced();
			VoteForSelf();
		}

		public override RaftEngineState State
		{
			get { return RaftEngineState.Candidate; }
		}

		public override void Handle(string source, RequestVoteResponse resp)
		{
			if (resp.VoteTerm != Engine.PersistentState.CurrentTerm)
			{
				_log.Info("Got a vote for election term {0} but current term is {1}, ignoring", resp.VoteTerm, Engine.PersistentState.CurrentTerm);
				return;
			}
			if (resp.CurrentTerm > Engine.PersistentState.CurrentTerm )
			{
				_log.Info("CandidateStateBehavior -> UpdateCurrentTerm called, there is a new leader, moving to follower state");
				Engine.UpdateCurrentTerm(resp.CurrentTerm, null); 
				return;
			}

			if (resp.VoteGranted == false)
			{
				_log.Info("Vote rejected from {0} trial: {1}", resp.From, resp.TrialOnly);
				return;
			}

			if(Engine.ContainedInAllVotingNodes(resp.From) == false) //precaution
			{
				_log.Info("Vote acepted from {0}, which isn't in our topology", resp.From);
				return;
			}

			if (resp.TrialOnly && _wonTrialElection) // note that we can't get a vote for real election when we get a trail, because the terms would be different
			{
				_log.Info("Got a vote for trial only from {0} but we already won the trial election for this round, ignoring", resp.From);
				return;
			}

			_votesForMyLeadership.Add(resp.From);
			_log.Info("Adding to my votes: {0} (current votes: {1})", resp.From, string.Join(", ", _votesForMyLeadership));

			if (Engine.CurrentTopology.HasQuorum(_votesForMyLeadership) == false)
			{
				_log.Info("Not enough votes for leadership, votes = {0}", _votesForMyLeadership.Any() ? string.Join(", ", _votesForMyLeadership) : "empty");
				return;
			}

			if (_wonTrialElection == false)
			{
				_wonTrialElection = true;
				_log.Info("Won trial election with {0} votes from {1}, now running for real", _votesForMyLeadership.Count, string.Join(", ", _votesForMyLeadership));
				StartElection();
				return;
			}
			
			Engine.SetState(RaftEngineState.Leader);
			_log.Info("Selected as leader, term = {0}", resp.CurrentTerm);
		}

    }
}