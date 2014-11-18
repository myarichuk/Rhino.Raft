// -----------------------------------------------------------------------
//  <copyright file="CandidateStateBehavior.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;
using Rhino.Raft.Messages;

namespace Rhino.Raft.Behaviors
{
	public class CandidateStateBehavior : AbstractRaftStateBehavior
    {
		private readonly HashSet<string> _votesForMyLeadership = new HashSet<string>();
		private readonly Random _random;
		private bool wonTrialElection;

		public CandidateStateBehavior(RaftEngine engine) : base(engine)
		{
			_random = new Random((int) (engine.Name.GetHashCode() + DateTime.UtcNow.Ticks));
			Timeout = _random.Next(engine.MessageTimeout / 2, engine.MessageTimeout);
			StartElection(runTrialElection: true);
		}

		private void VoteForSelf()
		{
			Engine.DebugLog.Write("Voting for myself in term {0}", Engine.PersistentState.CurrentTerm);
			Handle(Engine.Name,
				new RequestVoteResponse
				{
					CurrentTerm = Engine.PersistentState.CurrentTerm,
					VoteGranted = true,
					Message = String.Format("{0} -> Voting for myself", Engine.Name),
					From = Engine.Name,
					VoteTerm = Engine.PersistentState.CurrentTerm
				});
		}

		public override void HandleTimeout()
	    {
			Engine.DebugLog.Write("Timeout ({1:#,#;;0} ms) for elections in term {0}", Engine.PersistentState.CurrentTerm,
				  Timeout);

			Timeout = _random.Next(Engine.MessageTimeout / 2, Engine.MessageTimeout);
			StartElection(runTrialElection: true);
	    }

		private void StartElection(bool runTrialElection)
		{
			LastHeartbeatTime = DateTime.UtcNow;
			_votesForMyLeadership.Clear();
			Engine.AnnounceCandidacy(runTrialElection);
			wonTrialElection = runTrialElection == false;
			VoteForSelf();
		}

		public override RaftEngineState State
		{
			get { return RaftEngineState.Candidate; }
		}

		public override void Handle(string source,RequestVoteResponse resp)
		{
			if (resp.VoteTerm != Engine.PersistentState.CurrentTerm)
			{
				Engine.DebugLog.Write("Got a vote for election term {0} but current term is {1}, ignoring", resp.VoteTerm, Engine.PersistentState.CurrentTerm);
				return;
			}
			if (resp.CurrentTerm > Engine.PersistentState.CurrentTerm )
			{
				Engine.DebugLog.Write("CandidateStateBehavior -> UpdateCurrentTerm called, there is a new leader, moving to follower state");
				Engine.UpdateCurrentTerm(resp.CurrentTerm, null); 
				return;
			}

			if (resp.VoteGranted == false)
			{
				Engine.DebugLog.Write("Vote rejected from {0} trial: {1}", resp.From, resp.TrialOnly);
				return;
			}

			if(Engine.ContainedInAllVotingNodes(resp.From) == false) //precaution
			{
				Engine.DebugLog.Write("Vote acepted from {0}, which isn't in our topology", resp.From);
				return;
			}

			if (resp.TrialOnly && wonTrialElection) // note that we can't get a vote for real election when we get a trail, because the terms would be different
			{
				Engine.DebugLog.Write("Got a vote for trial only from {0} but we already won the trial election for this round, ignoring", resp.From);
				return;
			}

			_votesForMyLeadership.Add(resp.From);
			Engine.DebugLog.Write("Adding to my votes: {0} (current votes: {1})", resp.From, string.Join(", ", _votesForMyLeadership));

			if (Engine.CurrentTopology.HasQuorum(_votesForMyLeadership) == false)
			{
				Engine.DebugLog.Write("Not enough votes for leadership, votes = {0}", _votesForMyLeadership.Any() ? string.Join(", ", _votesForMyLeadership) : "empty");
				return;
			}

			if (wonTrialElection == false)
			{
				wonTrialElection = true;
				Engine.DebugLog.Write("Won trial election with {0} votes from {1}, now running for real", _votesForMyLeadership.Count, string.Join(", ", _votesForMyLeadership));
				StartElection(runTrialElection: false);
				return;
			}
			
			Engine.SetState(RaftEngineState.Leader);
			Engine.DebugLog.Write("Selected as leader, term = {0}", resp.CurrentTerm);
		}

    }
}